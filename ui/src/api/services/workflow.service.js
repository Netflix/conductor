/* eslint-disable no-restricted-globals */
const BaseService = require('./base.service');

class WorkflowService extends BaseService {
  async search(req) {
    const { freeText: reqFreeText = '', start: reqStart = '', h: reqH, q = '' } = req;

    const freeText = [];
    if (reqFreeText !== '') {
      freeText.push(reqFreeText);
    } else {
      freeText.push('*');
    }

    let h = '-1';
    if (reqH && reqH !== 'undefined' && reqH !== '') {
      h = reqH;
    }

    if (h !== '-1') {
      freeText.push(`startTime:[now-${h}h TO now]`);
    }

    let start = 0;
    if (!isNaN(reqStart)) {
      start = reqStart;
    }

    const url = `workflow/search?size=100&sort=startTime:DESC&freeText=${freeText.join(
      ' AND '
    )}&start=${start}&query=${q}`;

    const { results: hits, totalHits } = await this.get(url, req.token);

    return { result: { hits, totalHits } };
  }
}

module.exports = WorkflowService;
