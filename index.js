const core = require('@actions/core');
const { Client } = require('pg');
const axios = require('axios');

const parse = require('pg-connection-string').parse;
const { sendSpans } = require('./spans');

let queries = [
  'select * from  postgres_air.boarding_pass as b where passenger_id = 4484038',
  "SELECT * FROM postgres_air.account WHERE last_name = 'johns'",
  "/* DESC: A simple query on one table. It scans the entire table as there are no relevant index. Rows Returned: 66 Rows Filtered: ~257,268 Rows Read = ~257,268 + 66 = ~257,334 Plan Type: Actual */ SELECT * FROM postgres_air.account WHERE last_name = 'johns'",
  "SELECT * FROM postgres_air.account WHERE last_name = 'johns' limit 15",
  "SELECT * FROM postgres_air.boarding_pass WHERE update_ts::date BETWEEN '2020-08-10' AND '2020-08-17' LIMIT 100",
  "SELECT flight_id, scheduled_departure FROM postgres_air.flight f JOIN postgres_air.airport a ON departure_airport=airport_code AND iso_country='US'",
];

async function createTest(apiKey) {
  const body = {
    prName: `Metis-Queries-Performance-QA-${Date().split(' GMT')[0]}`,
    prId: 'noPrId',
    prUrl: 'noPrUrl',
  };
  const headers = {
    'x-api-key': apiKey,
  };

  try {
    await axios.post(backendUrl, body, { headers });
  } catch (error) {
    console.error(error);
  }
}

async function getQueryAndPlan(client, query) {
  const explainedQuery = 'EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, TIMING, FORMAT JSON) ' + query;
  const explainedQueryResult = await client.query(explainedQuery);
  const plan = explainedQueryResult.rows[0]['QUERY PLAN'][0];

  return {
    query,
    plan,
  };
}

async function createNewClient(dbConnection) {
  const client = new Client(dbConnection);
  return client;
}

async function connectClient(client) {
  await client.connect();
}

async function endClient(client) {
  await client.end();
}

async function run() {
  try {
    /*
      Parse connection string to object
    */
    let credentials = parse(core.getInput('db_connection_string'));

    /*
      Set actions vars from action input args
    */
    const metisApikey = core.getInput('metis_api_key');
    const dbConnection = {
      database: credentials.database,
      user: credentials.user,
      password: credentials.password,
      host: credentials.host,
      // ssl: config?.ssl || { rejectUnauthorized: false },
    };

    createTest(metisApikey);
    const client = await createNewClient(dbConnection);
    await connectClient(client);

    const analyzedQueries = await Promise.all(
      queries.map(async (query) => {
        const res = await getQueryAndPlan(client, query);
        return res;
      })
    );

    endClient(client);

    await sendSpans(metisApikey, analyzedQueries, dbConnection, core.getInput('metis_exporter_url'), core.getInput('target_url'));
  } catch (error) {
    console.error(error);
    core.setFailed(error);
  }
}

run();
