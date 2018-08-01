import BaseService from './base.service';

class SystemService extends BaseService {
  constructor() {
    super();

    const { EVENT_API_BASE_ROUTE = 'event' } = process.env;
    this.baseRoute = EVENT_API_BASE_ROUTE;
  }

  async list(token) {
    const result = await this.get(`${this.baseRoute}`, token);

    return result;
  }

  async executions(event, token) {
    const result = await this.get(`${this.baseRoute}/${event}?activeOnly=false, token`, token);

    return result;
  }
}

module.exports = SystemService;
