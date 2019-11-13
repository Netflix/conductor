import React, {Component} from 'react';
import {connect} from 'react-redux';
import http from '../../core/HttpClient';
import * as authHelper from '../../core/AuthHelper';

class Footer extends Component {

  constructor(props) {
    super(props);
    this.state = {
      sys: {},
      username: null
    };

    http.get('/api/sys/').then((data) => {
      this.state.sys = data.sys;
      window.sys = this.state.sys;
    });
  }

  handleLogoutClick() {
    this.props.dispatch(authHelper.authLogout());
  }

  render() {
    return (
      <div className="Footer navbar-fixed-bottom">
        <div className="row">
          <div className="col-md-4">
            <span className="Footer-text">Server: </span>
            <a href={this.state.sys.server} target="_new" className="small"
               style={{color: 'white'}}>{this.state.sys.server}</a>
          </div>
          <div className="col-md-4">
            <span className="Footer-text">User: </span>
            <span className="small" style={{color: 'white'}}>{this.props.username}</span>
          </div>
          <div className="col-md-4 right">
            <span className="Footer-text">Version: </span>
            <span className="small" style={{color: 'white'}}>{this.state.sys.version}</span>
            <span className="Footer-text"> | </span>
            <span className="Footer-text">Build Date: </span>
            <span className="small" style={{color: 'white'}}>{this.state.sys.buildDate}</span>
          </div>
        </div>
      </div>
    );
  }
}

export default connect(state => state.workflow)(Footer);