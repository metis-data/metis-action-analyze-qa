const axios = require('axios');
const { uuid } = require('uuidv4');
const core = require('@actions/core');

const sendSpansToBackend = async (queriesToSend, apiKey, metisExporterUrl) => {
  try {
    if (queriesToSend === 0) {
      console.log('No Spans To Send');
      return;
    }
    if (!apiKey) {
      console.debug('API Key doesnt exists');
    }
    core.info(`queries to send`);
    core.info(queriesToSend.length);
    core.info(`queries to send`);

    await sendMultiSpans(metisExporterUrl, apiKey, queriesToSend);
  } catch (error) {
    console.error(error);
  }
};

const makeSpan = async (query, queryType, plan, connection, prName) => {
  const span_id = uuid();
  const traceId = uuid();

  const duration = (plan && plan['Execution Time']) || 1;

  const timestamp = Date.now();
  const startDate = new Date(timestamp).toISOString();
  const endDate = new Date(timestamp + duration).toISOString();

  const vendor = 'metis-action-analyze-qa';

  const parsedPlan = JSON.stringify(plan);

  // get host name
  let hostName = vendor;
  try {
    hostName = connection.host;
  } catch (e) {}

  return {
    kind: 'SpanKind.CLIENT',
    name: 'SELECT postgres',
    links: [],
    events: [],
    status: {
      status_code: 'UNSET',
    },
    context: {
      span_id: span_id,
      trace_id: traceId,
    },
    end_time: '2023-06-05T14:56:50.534Z',
    start_time: '2023-06-05T14:56:50.533Z',
    duration: 1,
    resource: {
      'service.name': 'api-service',
      'metis.sdk.version': '67dee834d8b7eb0433640d45718759992dde0bb4',
      'metis.sdk.name': prName,
      'telemetry.sdk.name': 'Metis-Queries-Performance-QA-Mon-Jun-05-2023-09:38:25',
      'telemetry.sdk.version': '1.11.1',
      'telemetry.sdk.language': 'query-analysis',
      'app.tag.pr': prName,
    },
    parent_id: null,
    attributes: {
      'db.system': 'postgresql',
      'db.statement.metis': query,
      'net.peer.name': '127.0.0.1',
      'net.peer.port': 5432,
      'db.statement.metis.plan': parsedPlan,
    },
  };
};

const axiosPost = async (url, body, options) => {
  try {
    const res = await axios.post(url, body, options);
    return res;
  } catch (error) {
    console.log(error);
  }
};

async function sendMultiSpans(url, apiKey, spans, prName) {
  core.info(`spans length : ${spans.length}`);
  const spansString = spans.map((d) => d && JSON.stringify(d, null, 0));
  const response = [];
  for (let chuckedData of spansString) {
    const dataString = chuckedData ? JSON.stringify(chuckedData, null, 0) : undefined;
    if (dataString && dataString.length) {
      const options = {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'Content-Length': dataString.length,
          'x-api-key': apiKey,
        },
      };

      response.push(await axiosPost(url, dataString, options));
    }
  }

  return response;
}

const sendSpans = async (metisApikey, queriesAndPlans, connection, metisExporterUrl, prName) => {
  console.log(JSON.stringify(prName));
  const spans = await Promise.all(
    queriesAndPlans?.map(async (item) => {
      return await makeSpan(item.query, 'select', item.plan, connection, prName);
    })
  );

  sendSpansToBackend(spans, metisApikey, metisExporterUrl);
};

module.exports = { sendSpans };
