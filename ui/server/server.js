/* eslint-disable class-methods-use-this */
require('dotenv').config();
require('babel-polyfill');
const express = require('express');
const MiddlewareIndex = require('./middleware');

const WorkflowRoutes = require('./routes/workflow');
const MetadataRoutes = require('./routes/metadata');
const SystemRoutes = require('./routes/system');
const EventsRoutes = require('./routes/events');
const TaskRoutes = require('./routes/tasks');

class Main {
  init() {
    const app = express();
    const middlewareIndex = new MiddlewareIndex();

    this.preMiddlewareConfig(app, middlewareIndex);
    this.routesConfig(app);
    this.postMiddlewareConfig(app, middlewareIndex);
    this.startServer(app);
  }

  preMiddlewareConfig(app, middlewareIndex) {
    middlewareIndex.before(app);
  }

  routesConfig(app) {
    app.use(express.static('server/public/build'));

    new WorkflowRoutes().init(app);
    new MetadataRoutes().init(app);
    new TaskRoutes().init(app);
    new SystemRoutes().init(app);
    new EventsRoutes().init(app);
  }

  postMiddlewareConfig(app, middlewareIndex) {
    middlewareIndex.after(app);
  }

  startServer(app) {
    const server = app.listen(process.env.NODE_PORT || 5000, () => {
      const { address: host = 'localhost', port } = server.address();
      const serverStartMessage = `Workflow UI listening at http://${host}:${port}`;

      console.info(serverStartMessage);

      if (process.send) {
        process.send('online');
      }
    });
  }
}

const main = new Main();

main.init();
