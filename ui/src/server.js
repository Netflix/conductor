import 'babel-polyfill';
import express from 'express';
import React from 'react';
import log4js from 'log4js';
import bodyParser from 'body-parser';
import { serverConfig } from "./core/ServerConfig";

log4js.addLayout('kibana', config => logEvent => {
  const normalizeText = text => {
    return text.replace(/"/g, '\'').replace(/\n/g, '||');
  };

  return 'timestamp=' + new Date().toISOString() +
    ` log-level="${logEvent.level.levelStr}"` +
    ` severity="${logEvent.level.level}"` +
    ` logger="${logEvent.categoryName}"` +
    ` text="${normalizeText(logEvent.data.join(' '))}"`;
});

log4js.configure({
  appenders: {
    out: { type: 'stdout', layout: { type: 'kibana', separator: '\n' } }
  },
  categories: {
    default: { appenders: ['out'], level: 'debug' }
  }
});

process.on('uncaughtException', function (err) {
  logger.fatal(err);
  process.exit(1);
});

const logger = log4js.getLogger('server.main');

const v1API = require('./api/v1');
const wfeAPI = require('./api/wfe');
const sysAPI = require('./api/sys');
const authAPI = require('./api/auth.js');
const eventsAPI = require('./api/events');

const app = express();
app.use(express.static('public'));
app.use('/v1', v1API);
app.use('/api/wfe', wfeAPI);
app.use('/api/sys', sysAPI);
app.use('/api/events', eventsAPI);
app.use('/api/auth', bodyParser.urlencoded({ extended: false })); // parse application/x-www-form-urlencoded
app.use('/api/auth', bodyParser.json()); // parse application/json
app.use('/api', authAPI);

const port = serverConfig.port();
app.listen(port, function (err) {
  if (err) {
    logger.fatal(err);
    process.exit(-1);
  }

  logger.info('Workflow UI listening at ' + port);
});
