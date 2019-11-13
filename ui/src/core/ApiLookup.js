import DnsResolver from "./DnsResolver";

const wfService = process.env.WF_SERVICE;
const wfServer = process.env.WF_SERVER;

const ApiLookup = {

  lookup: () => new Promise((resolve, reject) => {
    if (!wfService) {
      resolve(wfServer);
      return;
    }

    new DnsResolver().resolve(wfService, results => {
      if (results && results.length > 0) {
        let instance = results[0];
        resolve('http://' + instance.name + ':' + instance.port + '/api/');
      } else {
        reject(`Dns lookup failed for host ${wfService}. No matches found.`);
      }
    }, err => reject(err));

  })
};

export default ApiLookup;