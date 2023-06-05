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

  const newClientSpan = {
    kind: 'SpanKind.CLIENT',
    name: 'SELECT program',
    links: [],
    events: [],
    status: {
      status_code: 'UNSET',
    },
    context: {
      span_id: '0x2c2496FDCdA64AAa',
      trace_id: '0xD8a6b802b0B27e88BB4BfB24ECAa6Fcf',
      trace_state: '[]',
    },
    duration: 119,
    end_time: '2023-06-02T18:12:49.631Z',
    resource: {
      'app.tag.pr': prName,
      'host.name': 'mr-meeseeks',
      'service.name': hostName,
      'service.version': '1.0.0',
      'telemetry.sdk.name': vendor,
      'telemetry.sdk.version': '1.11.1',
      'telemetry.sdk.language': vendor,
    },
    parent_id: '0xccBCaBC4Af076ABD',
    attributes: {
      'db.name': 'dobby is a free elf',
      'db.system': 'postgresql',
      'net.peer.name': 'dev.chiq.ai',
      'db.statement.metis':
        "SELECT departure_airport, booking_id, is_returning FROM postgres_air.booking_leg bl JOIN postgres_air.flight f USING (flight_id) WHERE departure_airport IN (SELECT airport_code FROM postgres_air.airport WHERE iso_country='US')",
      'db.statement.metis.plan':
        '{ "Plan": { "Node Type": "Hash Join", "Parallel Aware": false, "Join Type": "Inner", "Startup Cost": 19596.55, "Total Cost": 610393.76, "Plan Rows": 3788277, "Plan Width": 9, "Output": ["f.departure_airport", "bl.booking_id", "bl.is_returning"], "Inner Unique": false, "Hash Cond": "(bl.flight_id = f.flight_id)", "Plans": [ { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Parallel Aware": false, "Relation Name": "booking_leg", "Schema": "postgres_air", "Alias": "bl", "Startup Cost": 0.00, "Total Cost": 310506.66, "Plan Rows": 17893566, "Plan Width": 9, "Output": ["bl.booking_id", "bl.is_returning", "bl.flight_id"] }, { "Node Type": "Hash", "Parent Relationship": "Inner", "Parallel Aware": false, "Startup Cost": 17223.60, "Total Cost": 17223.60, "Plan Rows": 144636, "Plan Width": 8, "Output": ["f.departure_airport", "f.flight_id"], "Plans": [ { "Node Type": "Hash Join", "Parent Relationship": "Outer", "Parallel Aware": false, "Join Type": "Inner", "Startup Cost": 20.09, "Total Cost": 17223.60, "Plan Rows": 144636, "Plan Width": 8, "Output": ["f.departure_airport", "f.flight_id"], "Inner Unique": true, "Hash Cond": "(f.departure_airport = airport.airport_code)", "Plans": [ { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Parallel Aware": false, "Relation Name": "flight", "Schema": "postgres_air", "Alias": "f", "Startup Cost": 0.00, "Total Cost": 15398.76, "Plan Rows": 683176, "Plan Width": 8, "Output": ["f.flight_id", "f.flight_no", "f.scheduled_departure", "f.scheduled_arrival", "f.departure_airport", "f.arrival_airport", "f.status", "f.aircraft_code", "f.actual_departure", "f.actual_arrival", "f.update_ts"] }, { "Node Type": "Hash", "Parent Relationship": "Inner", "Parallel Aware": false, "Startup Cost": 18.33, "Total Cost": 18.33, "Plan Rows": 141, "Plan Width": 4, "Output": ["airport.airport_code"], "Plans": [ { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Parallel Aware": false, "Relation Name": "airport", "Schema": "postgres_air", "Alias": "airport", "Startup Cost": 0.00, "Total Cost": 18.33, "Plan Rows": 141, "Plan Width": 4, "Output": ["airport.airport_code"], "Filter": "(airport.iso_country = \'US\'::text)" } ] } ] } ] } ] } }',
    },
    start_time: '2023-06-02T18:12:49.512Z',
  };

  return newClientSpan;
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
