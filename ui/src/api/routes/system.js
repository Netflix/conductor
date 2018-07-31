import SystemService from '../services/system.service';

class SystemRoutes {
  constructor() {
    this.systemService = new SystemService();
  }

  init(app) {
    app.get('/api/sys/', async (req, res, next) => {
      try {
        const { token } = req;

        const result = await this.systemService.adminConfig(token);
        console.log('result =>', result);
        res.status(200).send(result);
      } catch (err) {
        next(err);
      }
    });
  }
}

module.exports = SystemRoutes;
