import path from "node:path";
import { expectDefined } from "@openclaw/normalization-core/expect";
import { describe, expect, it, vi } from "vitest";
import {
  appendTranscriptMessage,
  bindSessionPendingInputSources,
  listSessionPendingInputs,
  stageSessionPendingInput,
  upsertSessionEntryCore,
} from "../../../config/sessions/session-accessor.js";
import { withOpenClawTestState } from "../../../test-utils/openclaw-test-state.js";
import type { AgentMessage } from "../../runtime/index.js";
import { guardSessionManager } from "../../session-tool-result-guard-wrapper.js";
import type { AgentSession } from "../../sessions/index.js";
import { SessionManager } from "../../sessions/session-manager.js";
import { makeAssistantMessageFixture } from "../../test-helpers/assistant-message-fixtures.js";
import { prepareEmbeddedAttemptSessionBoundary } from "./attempt-session-prepare.js";

function createActiveSession(messages: AgentMessage[]) {
  const activeSession = {
    agent: { state: { messages }, convertToLlm: vi.fn((input: AgentMessage[]) => input) },
  } as unknown as Pick<AgentSession, "agent">;
  return { activeSession };
}

describe("live pending inputs at the attempt boundary", () => {
  it.each([
    { metadata: false, collected: false, excluded: false },
    { metadata: true, collected: false, excluded: false },
    { metadata: true, collected: true, excluded: false },
    { metadata: false, collected: false, excluded: true },
  ])(
    "preserves a live queued turn across announce ($metadata, $collected, $excluded)",
    async ({ metadata, collected, excluded }) => {
      await withOpenClawTestState({ label: "live-input-orphan" }, async (state) => {
        const target = {
          agentId: "main",
          sessionId: "live-input-session",
          sessionKey: "agent:main:live-input",
          storePath: path.join(state.sessionsDir(), "sessions.json"),
        };
        await upsertSessionEntryCore(target, { sessionId: target.sessionId, updatedAt: 1 });
        const manager = guardSessionManager(SessionManager.open(target, state.workspaceDir), {
          runId: "announce-run",
        });
        const prior = makeAssistantMessageFixture({
          content: [{ type: "text", text: "prior reply" }],
          stopReason: "stop",
          timestamp: 1,
        });
        manager.appendMessage(prior);
        const message: Parameters<typeof stageSessionPendingInput>[1]["message"] = {
          role: "user" as const,
          content: "queued user request",
          timestamp: 2,
          idempotencyKey: "queued-run:user",
          ...(excluded ? { excludeFromContext: true } : {}),
        };
        const source = expectDefined(
          await stageSessionPendingInput(target, {
            runId: "queued-run",
            message,
            assertCurrent: () => {},
          }),
          "Expected queued receipt",
        );
        const receipt = collected
          ? expectDefined(
              bindSessionPendingInputSources([source], {
                ...message,
                idempotencyKey: "collected-run:user",
              }),
              "Expected aggregate receipt",
            )
          : source;
        try {
          const promoted = expectDefined(
            await receipt.run(() => appendTranscriptMessage(target, { message: receipt.message })),
            "Expected promoted input",
          );
          expect(promoted).toMatchObject({ appended: true });
          expect(listSessionPendingInputs(target).items).toEqual([]);
          const announce = guardSessionManager(
            SessionManager.openBounded(target, { maxBytes: 8192, maxEvents: 30 }),
            { runId: "announce-run" },
          );
          if (metadata) {
            await announce.appendThinkingLevelChange("low");
            await announce.appendModelChange("openai", "synthetic-model");
          }
          const { activeSession } = createActiveSession(announce.buildSessionContext().messages);
          const boundary = await prepareEmbeddedAttemptSessionBoundary({
            activeSession,
            attempt: { prompt: "announce child result", trigger: "user" },
            getUserTranscriptContexts: () => undefined,
            isRawModelRun: false,
            preparedUserTurnMessage: undefined,
            sessionManager: announce,
            setActiveSessionSystemPrompt: vi.fn(),
          });
          announce.appendMessage({
            role: "user",
            content: boundary.orphanRepair?.contextEnginePrompt ?? "announce child result",
            timestamp: 3,
          });
          announce.appendMessage(
            makeAssistantMessageFixture({
              content: [{ type: "text", text: "child result delivered" }],
              stopReason: "stop",
              timestamp: 4,
            }),
          );
          // Replay through the original receipt must keep the exact promoted anchor.
          await expect(
            receipt.run(() => appendTranscriptMessage(target, { message: receipt.message })),
          ).resolves.toMatchObject({ appended: false, messageId: promoted.messageId });
          expect(boundary.orphanRepair).toBeUndefined();
          expect(activeSession.agent.state.messages).toMatchObject([prior]);
          const rebuilt = announce.buildSessionContext().messages;
          expect(JSON.stringify(await activeSession.agent.convertToLlm(rebuilt))).not.toContain(
            "queued user request",
          );
          const reopened = SessionManager.openBounded(target, { maxBytes: 8192, maxEvents: 30 });
          expect(
            reopened.getBranch().filter((entry) => entry.id === promoted.messageId),
          ).toHaveLength(excluded ? 0 : 1);
          expect(
            reopened.buildSessionContext().messages.filter((entry) => entry.role === "user"),
          ).toMatchObject(
            excluded
              ? [{ content: "announce child result" }]
              : [{ content: "queued user request" }, { content: "announce child result" }],
          );
        } finally {
          receipt.finish("interrupted");
        }
      });
    },
  );
});
