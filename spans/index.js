const axios = require('axios');
const { uuid } = require('uuidv4');
const core = require('@actions/core');

const sendSpansToBackend = async (queriesToSend, apiKey, metisExporterUrl) => {
  try {
    if (queriesToSend === 0) {
      core.info('No Spans To Send');
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

const generateServerSpan = (traceId, routeName, prName) => {
  const span_id = uuid();
  return {
    kind: 'SpanKind.SERVER',
    name: routeName,
    links: [],
    events: [],
    status: {
      status_code: 'UNSET',
    },
    context: {
      span_id: span_id,
      trace_id: traceId,
      trace_state: '[]',
    },
    duration: 880,
    end_time: '2023-06-03T19:45:02.325Z',
    resource: {
      'app.tag.pr': prName,
      'host.name': 'legal-erection.net',
      'service.name': 'notification',
      'service.version': '6.0.9',
      'telemetry.sdk.name': 'opentelemetry',
      'telemetry.sdk.version': '1.11.1',
      'telemetry.sdk.language': 'python',
    },
    parent_id: null,
    attributes: {
      'http.url': 'http://exemplary-slider.name',
      'http.host': 'test:None',
      'http.route': routeName,
      'http.flavor': '1.1',
      'http.method': 'POST',
      'http.scheme': 'http',
      'http.target': routeName,
      'net.peer.ip': '127.0.0.1',
      'net.peer.port': 123,
      'net.host.name': 'localhost',
      'track.by.metis': true,
      'http.user_agent': 'python-httpx/0.21.3',
      'http.server_name': 'test',
      'http.status_code': 201,
      'http.response.header.content_type': ['application/json'],
      'http.response.header.content_length': ['2'],
    },
    start_time: '2023-06-03T19:45:01.445Z',
  };
};

const makeSpan = async (item, queryType, connection, prName, traceId) => {
  const span_id = uuid();

  const duration = (item.plan && item.plan['Execution Time']) || 1;

  const timestamp = Date.now();
  const startDate = new Date(timestamp).toISOString();
  const endDate = new Date(timestamp + duration).toISOString();

  const vendor = 'metis-action-analyze-qa';

  const parsedPlan = JSON.stringify(item.plan);

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
    end_time: endDate,
    start_time: startDate,
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
      'db.statement.metis': item.query,
      'net.peer.name': '127.0.0.1',
      'net.peer.port': 5432,
      'db.statement.metis.plan': parsedPlan,
    },
  };
};

const axiosPost = async (url, body, options) => {
  try {
    const res = await axios.post(url, body, options);
    console.info(res)
    core.info(`send span to backend successfully`);
    return res;
  } catch (error) {
    console.log(error);
    core.info(`failed to send span`);
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

const sendSpans = async (metisApikey, queriesAndPlans, connection, metisExporterUrl, prName, useRoute) => {
  core.info(JSON.stringify(prName));
  core.info(core.getInput('useRoute') ? 'send query span with server span' : 'send only query span');
  let arr = [];
  const spans = await Promise.all(
    queriesAndPlans?.map(async (item, idx) => {
      if ((useRoute && idx % 2 !== 0 && core.getInput('useRoute')) || core.getInput('qaMode')) {
        arr.push(generateServerSpan(item?.traceId, item?.route, prName));
      }
      return await makeSpan(item, 'select', connection, prName, item?.traceId);
    })
  );
  core.info(JSON.stringify(arr));
  sendSpansToBackend([...arr, ...spans], metisApikey, metisExporterUrl);
};

module.exports = { sendSpans };
