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

  // get host name
  let hostName = vendor;
  try {
    hostName = connection.host;
  } catch (e) {}

  const resource = {
    'metis.sdk.name': prName,
    'service.name': hostName,
    'service.version': 'or0.000000000000001%',
    'telemetry.sdk.name': vendor,
    'telemetry.sdk.version': 'or0.000000000000000000000000001%',
    'telemetry.sdk.language': vendor,
  };

  return {
    parent_id: null,
    name: queryType || 'REPL',
    kind: 'SpanKind.CLIENT',
    duration: 2,
    start_time: startDate,
    end_time: endDate,
    attributes: {
      'db.name': connection?.database,
      'db.user': connection?.user,
      'db.system': 'postgres',
      'db.operation': queryType,
      'db.statement': query,
      'db.statement.metis': query + `/*traceparent=${traceId}-${span_id}*/''`,
      'db.statement.metis.plan': /*JSON.stringify(plan)*/ "{\"Plan\":{\"Node Type\":\"Index Scan\",\"Parallel Aware\":false,\"Scan Direction\":\"Forward\",\"Index Name\":\"flight_pkey\",\"Relation Name\":\"flight\",\"Schema\":\"postgres_air\",\"Alias\":\"flight\",\"Startup Cost\":0.42,\"Total Cost\":8.44,\"Plan Rows\":1,\"Plan Width\":71,\"Output\":[\"flight_id\",\"flight_no\",\"scheduled_departure\",\"scheduled_arrival\",\"departure_airport\",\"arrival_airport\",\"status\",\"aircraft_code\",\"actual_departure\",\"actual_arrival\",\"update_ts\"],\"Index Cond\":\"(flight.flight_id = 108340)\",\"Shared Hit Blocks\":0,\"Shared Read Blocks\":0,\"Shared Dirtied Blocks\":0,\"Shared Written Blocks\":0,\"Local Hit Blocks\":0,\"Local Read Blocks\":0,\"Local Dirtied Blocks\":0,\"Local Written Blocks\":0,\"Temp Read Blocks\":0,\"Temp Written Blocks\":0,\"I/O Read Time\":0,\"I/O Write Time\":0},\"Planning\":{\"Shared Hit Blocks\":0,\"Shared Read Blocks\":0,\"Shared Dirtied Blocks\":0,\"Shared Written Blocks\":0,\"Local Hit Blocks\":0,\"Local Read Blocks\":0,\"Local Dirtied Blocks\":0,\"Local Written Blocks\":0,\"Temp Read Blocks\":0,\"Temp Written Blocks\":0,\"I/O Read Time\":0,\"I/O Write Time\":0},\"Planning Time\":0.066}",
      'net.peer.name': connection?.host,
      'net.peer.ip': connection?.host,
    },
    status: {
      status_code: 'UNSET',
    },
    context: {
      span_id: span_id,
      trace_id: traceId,
    },
    resource,
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
  console.log(JSON.stringify(queriesAndPlans[0].plan));
  const spans = await Promise.all(
    queriesAndPlans?.map(async (item) => {
      return await makeSpan(item.query, 'select', item.plan, connection, prName);
    })
  );

  sendSpansToBackend(spans, metisApikey, metisExporterUrl);
};

module.exports = { sendSpans };
