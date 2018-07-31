import BaseService from './base.service';

class SystemService extends BaseService {
  async list(token) {
    const result = await this.get('event', token);

    return result;
  }

  async executions(event, token) {
    const result = await this.get(`/${event}?activeOnly=false, token`, token);

    return result;
  }
}

module.exports = SystemService;
