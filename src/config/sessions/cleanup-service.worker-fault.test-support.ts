import type { WorkerOptions } from "node:worker_threads";

export type CleanupDeleteFault = { databasePath: string; sessionId: string; message: string };

const deleteFaultPreload = `
  import { realpathSync } from "node:fs";
  import { DatabaseSync } from "node:sqlite";
  import { workerData } from "node:worker_threads";
  const fault = workerData.cleanupDeleteFault;
  const prepare = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function(sql) {
    const statement = prepare.call(this, sql);
    if (sql === 'delete from "session_windows" where "session_id" = ?' &&
        this.location() && realpathSync(this.location()) === realpathSync(fault.databasePath)) {
      const database = this;
      const run = statement.run.bind(statement);
      statement.run = (...args) => {
        if (args.length === 1 && args[0] === fault.sessionId) {
          if (!database.isTransaction) throw new Error('cleanup fault requires the real deletion transaction');
          const literal = (value) => "'" + value.replaceAll("'", "''") + "'";
          database.exec('CREATE TEMP TRIGGER fail_cleanup_window_delete BEFORE DELETE ON main.session_windows ' +
            'WHEN OLD.session_id = ' + literal(fault.sessionId) + ' BEGIN SELECT RAISE(ABORT, ' +
            literal(fault.message) + '); END;');
        }
        return run(...args);
      };
    }
    return statement;
  };
`;

/** Keep fault injection on the real worker connection without altering its admitted main schema. */
export function withCleanupDeleteFault(
  options: WorkerOptions | undefined,
  fault: CleanupDeleteFault | undefined,
): WorkerOptions | undefined {
  return fault
    ? {
        ...options,
        execArgv: [
          ...(options?.execArgv ?? []),
          "--import",
          `data:text/javascript,${encodeURIComponent(deleteFaultPreload)}`,
        ],
        workerData: { ...options?.workerData, cleanupDeleteFault: fault },
      }
    : options;
}
