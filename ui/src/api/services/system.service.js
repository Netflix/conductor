import BaseService from './base.service';

class SystemService extends BaseService {
  constructor() {
    super();

    const { ADMIN_API_BASE_ROUTE = 'admin' } = process.env;
    this.baseRoute = ADMIN_API_BASE_ROUTE;
  }

  async adminConfig(token) {
    const { data: config } = await this.get(`${this.baseRoute}/config`, token);

    const result = {
      ...config,
      server: process.env.WF_SERVER,
      env: process.env
    };
    return { sys: result };
  }
}

module.exports = SystemService;
