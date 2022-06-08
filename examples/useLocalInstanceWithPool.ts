import { SnowflakePool as SF } from 'snowflake-sdk-promise'

export class Snowflake {
  static instance: Snowflake = null

  public static getInstance() {
    if (!Snowflake.instance) {
      Snowflake.instance = new Snowflake()
    }

    return Snowflake.instance
  }

  protected snowflake: SF

  private constructor(
    private readonly accountId = process.env.SNOWFLAKE_ACCOUNT_ID,
    private readonly password = process.env.SNOWFLAKE_PASSWORD || 'unknown',
  ) {
    this.snowflake = new SF(
      {
        account: `${this.accountId}.eu-west-1`,
        username: 'YOUR_USERNAME',
        password: this.password,
        warehouse: 'YOUR_WAREHOUSE',
        database: 'FIVETRAN_SCRIBER_TEST',
        schema: 'PUBLIC',
        role: 'YOUR_ROLE',
      },
      {
        logLevel: 'ERROR',
      },
      {
        insecureConnect: true,
      },
    )
  }

  async query(query: string, binds?: any[]) {
    return await this.snowflake.execute(query, binds)
  }
}


setTimeout(async () => {
  const snowflake = Snowflake.getInstance()
  const schema = 'sendgrid_events'

  const values = await snowflake.query(
    `SELECT evt.custom_subscriber_id           AS "subscriber_id",
    (SELECT Count(*)
     FROM   ${schema}.event
     WHERE  event.custom_subscriber_id = evt.custom_subscriber_id
            AND event = 'open')      AS "total_opens",
    (SELECT Count(*)
     FROM   ${schema}.event
     WHERE  event.custom_subscriber_id = evt.custom_subscriber_id
            AND event = 'delivered') AS "total_received"
    FROM   ${schema}.event AS evt
    GROUP  BY evt.custom_subscriber_id`
  )

  console.log(values)

}, 3000)
