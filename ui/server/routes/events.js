const EventsService = require('../services/events.service');

class SystemRoutes {
  constructor() {
    this.eventsService = new EventsService();
  }

  init(app) {
    app.get('/api/events/', async (req, res, next) => {
      try {
        const { token } = req;
        const result = await this.eventsService.list(token);
        res.status(200).send(result);
      } catch (err) {
        next(err);
      }
    });

    app.get('/api/events/executions', async (req, res, next) => {
      try {
        const {
          params: { event },
          token
        } = req;

        const result = await this.eventsService.executions(event, token);
        res.status(200).send(result);
      } catch (err) {
        next(err);
      }
    });
  }
}

module.exports = SystemRoutes;
