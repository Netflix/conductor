/*! React Starter Kit | MIT License | http://www.reactstarterkit.com/ */

import request from 'superagent';
import { canUseDOM } from 'fbjs/lib/ExecutionEnvironment';

function getUrl(path) {
  if (path.startsWith('http') || canUseDOM) {
    return path;
  }

  return process.env.WEBSITE_HOSTNAME ?
    `http://${process.env.WEBSITE_HOSTNAME}${path}` :
    `http://127.0.0.1:${global.server.get('port')}${path}`;
}

const HttpClient = {

  get: (path) => new Promise((resolve, reject) => {
    request
      .get(getUrl(path))
      .accept('application/json')
      .end((err, res) => {
        if (err) {
          //if (err.status === 404) {
          //  resolve(null);
          //} else {
            reject(err);
          //}
        } else {
          resolve(res.body);
        }
      });
  }),

  post: (path, data, token) => new Promise((resolve, reject) => {
    let req = request
      .post(path, data)
      .set('Accept', 'application/json');
    if (token) {
      req.set('Authorization', 'Bearer ' + token);
    }
    req.end((err, res) => {
        if (err || !res.ok) {
          console.error('Error on post! ' + res);
          reject(err);
        } else {
          if(res.body){
            resolve(res.body);
          }else{
            resolve(res);
          }

        }
      });
  }),

  postPlain: (path, data, token) => new Promise((resolve, reject) => {
    let req = request
      .post(path, data)
      .set('Accept', 'text/plain');
    if (token) {
      req.set('Authorization', 'Bearer ' + token);
    }
    req.end((err, res) => {
      if (err || !res.ok) {
        console.error('Error on post! ' + res);
        reject(err);
      } else {
        if(res.body){
          resolve(res.body);
        }else{
          resolve(res);
        }

      }
    });
  }),

  put: (path, data, token) => new Promise((resolve, reject) => {
    let req = request
      .put(path, data)
      .set('Accept', 'application/json');
    if (token) {
      req.set('Authorization', 'Bearer ' + token);
    }
    req.end((err, res) => {
      if (err || !res.ok) {
        console.error('Error on post! ' + res);
        reject(err);
      } else {
        resolve(res.body);
      }
    });
  }),

  delete: (path, data, token) => new Promise((resolve, reject) => {
    let req = request
      .del(path, data)
      .set('Accept', 'application/json');
    if (token) {
      req.set('Authorization', 'Bearer ' + token);
    }
    req.end((err, res) => {
      if (err || !res.ok) {
        console.error('Error on post! ' + err);
        reject(err);
      } else {
        resolve(res);
      }
    });
  })
};

export default HttpClient;
