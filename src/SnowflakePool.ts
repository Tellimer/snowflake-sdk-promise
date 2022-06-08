import * as SDK from 'snowflake-sdk';
import { Pool } from 'generic-pool';
import { Statement } from './Statement';
import { ConfigureOptions } from './types/ConfigureOptions';
import { ConnectionOptions } from './types/ConnectionOptions';
import { ExecuteOptions } from './types/ExecuteOptions';
import { LoggingOptions } from './types/LoggingOptions';

export class SnowflakePool {
  private readonly sdkConnectionPool: Pool<SDK.Connection>;
  private readonly logSql: (sqlText: string) => void;

  /**
   * Creates a new Snowflake instance.
   *
   * @param connectionOptions The Snowflake connection options
   * @param loggingOptions Controls query logging and SDK-level logging
   * @param configureOptions Additional configuration options
   */
  constructor(
    connectionOptions: ConnectionOptions,
    loggingOptions: LoggingOptions = {},
    configureOptions?: ConfigureOptions | boolean
  ) {
    if (loggingOptions && loggingOptions.logLevel) {
      SDK.configure({ logLevel: loggingOptions.logLevel });
    }
    this.logSql = (loggingOptions && loggingOptions.logSql) || null;

    // For backward compatibility, configureOptions is allowed to be a boolean, but it’s
    // ignored. The new default settings accomplish the same thing as the old
    // `insecureConnect` boolean.

    if (typeof configureOptions === 'boolean') {
      console.warn(
        '[snowflake-promise] the insecureConnect boolean argument is deprecated; ' +
          'please remove it or use the ocspFailOpen configure option'
      );
    } else if (typeof configureOptions === 'object') {
      SDK.configure(configureOptions);
    }

    this.sdkConnectionPool = SDK.createPool(connectionOptions, {max: 10, min: 0});
  }

  /** Create a Statement. */
  createStatement(options: ExecuteOptions) {
    return this.sdkConnectionPool.use(async (clientConnection) => new Statement(clientConnection, options, this.logSql))
  }

  /** A convenience function to execute a SQL statement and return the resulting rows. */
  async execute(sqlText: string, binds?: any[]) {
    const stmt = await this.createStatement({ sqlText, binds });
    stmt.execute();
    return stmt.getRows();
  }
}
