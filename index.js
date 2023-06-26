const core = require('@actions/core');
const { Client } = require('pg');
const axios = require('axios');
const { uuid } = require('uuidv4');
const parse = require('pg-connection-string').parse;
const { sendSpans } = require('./spans');

const qaMode = core.getInput('qaMode') !== undefined 

const date = new Date();
const options = { year: 'numeric', month: 'short', day: '2-digit' };
const formattedDate = date.toLocaleDateString('en-US', options);

const currentMonth = date.getMonth();
const currentDay = date.getDay();
const currentHours = date.getHours();
const seconds = currentDate.getSeconds();

const month = currentMonth + 1;
const prName = core.getInput('qaMode') === 'true' ? `QA-${Date().split(' GMT')[0].replaceAll(' ', '-')}` :  `PR_version_${month}.${currentDay}.${currentHours}.${seconds}`;




async function createTest(apiKey, backendUrl) {
  const body = {
    prName: prName,
    prId: 'noPrId',
    prUrl: 'noPrUrl',
  };
  const headers = {
    'x-api-key': apiKey,
  };

  try {
    await axios.post(backendUrl + '/api/tests/create', body, { headers });
  } catch (error) {
    console.error(error);
  }
}

async function getQueryAndPlan(client, query, isActual) {
  const explainedQuery = `EXPLAIN (${isActual ? 'ANALYZE,' : ''} COSTS, VERBOSE, BUFFERS, ${isActual ? 'TIMING,' : ''} FORMAT JSON) ${query}`;
  const explainedQueryResult = await client.query(explainedQuery);
  const plan = explainedQueryResult.rows[0]['QUERY PLAN'][0];
  core.info(`Run query: ${query} in ${isActual ? 'actual mode' : 'estimated mode'}`);
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
    let inputQueries = [];
    /*
      Get Input Queries If Exists
    */
    if (core.getInput('queries') && core.getInput('queries') !== '[]') {
      inputQueries = JSON.parse(core.getInput('queries')).map((item) => item);
    }
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

    createTest(metisApikey, core.getInput('target_url'));
    const client = await createNewClient(dbConnection);
    await connectClient(client);

    const actualAnalyzedQueries = await Promise.all(
      inputQueries.map(async (item) => {
        const res = await getQueryAndPlan(client, item.query, true);
        return {
          route: item?.route,
          query: res.query,
          plan: res.plan,
        };
      })
    );

    const estimatedAnalyzedQueries = await Promise.all(
      inputQueries.map(async (item) => {
        const res = await getQueryAndPlan(client, item.query, false);
        return {
          route: item?.route,
          query: res.query,
          plan: res.plan,
        };
      })
    );
    let queriesToBeAnalyzed = [];

    if (core.getInput('qaMode') === 'true') {
      console.info('qa-mode');
      actualAnalyzedQueries.map((item, idx) => {
        const traceId = uuid();
        queriesToBeAnalyzed.push({ ...item, traceId });
        queriesToBeAnalyzed.push({ ...estimatedAnalyzedQueries[idx], traceId });
      });
    } else {
      console.info('demo-mode');
      queriesToBeAnalyzed = actualAnalyzedQueries.map((item) => {
        const traceId = uuid();
        return { ...item, traceId };
      });
    }

    endClient(client);

    await sendSpans(metisApikey, queriesToBeAnalyzed, dbConnection, core.getInput('metis_exporter_url'), prName, core.getInput('useRoute'));
  } catch (error) {
    console.error(error);
    core.setFailed(error);
  }
}

run();
