import React, { PropTypes, Component } from 'react';
import { Link } from 'react-router'
import { connect } from 'react-redux';
import http from '../../core/HttpClient';

class Footer extends Component {

  constructor(props) {
    super(props);
    this.state = {
      sys: {}
    };

    http.get('/api/sys/').then((data) => {
      this.state = {
        sys: data.sys
      };
      window.sys = this.state.sys;
    });
  }
  render() {
    return (
      <div className="Footer navbar-fixed-bottom">
        <div className="Footer-container left">
          <span className="Footer-text">Server: </span><a href={this.state.sys.server} target="_new" className="small" style={{color:'white'}}>{this.state.sys.server}</a>
          <span style={{float:'right'}}>
          <span className="Footer-text">Version: {this.state.sys.version} | Build Date: {this.state.sys.buildDate}</span>
          </span>
        </div>
      </div>
    );
  }

}

export default connect(state => state.workflow)(Footer);
