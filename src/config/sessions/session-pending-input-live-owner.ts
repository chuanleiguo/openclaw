import { resolveOpenClawAgentSqlitePath } from "../../state/openclaw-agent-db.js";
import type { SessionPendingInputOwner } from "./session-accessor.sqlite-pending-inputs.js";
import {
  toDatabaseOptions,
  type ResolvedTranscriptScope,
} from "./session-accessor.sqlite-scope.js";

/** A promoted input can outlive its pending row while another turn still owns it. */
export function hasForeignLivePendingInputOwner(params: {
  scope: ResolvedTranscriptScope;
  entryId: string;
  liveOwners: Iterable<SessionPendingInputOwner>;
  currentOwner: SessionPendingInputOwner | undefined;
  assertCurrent: (owner: SessionPendingInputOwner) => void;
}): boolean {
  const databasePath = resolveOpenClawAgentSqlitePath(toDatabaseOptions(params.scope));
  for (const source of params.liveOwners) {
    const owner = source.promotedOwner ?? source;
    if (
      owner === params.currentOwner ||
      owner.databasePath !== databasePath ||
      owner.sessionId !== params.scope.sessionId ||
      owner.sessionKey !== params.scope.sessionKey ||
      owner.transcriptInputId !== params.entryId
    ) {
      continue;
    }
    try {
      params.assertCurrent(owner);
      return true;
    } catch {
      // Finished, cancelled, or superseded turns no longer protect an orphan.
    }
  }
  return false;
}
