import React from 'react';
import packageJSON from '../../package.json';
import Footer from './common/Footer';
import ErrorPage from './common/Error'
import LeftMenu from './common/LeftMenu'
import {connect} from 'react-redux';
import * as authHelper from '../core/AuthHelper';

const App = React.createClass({

  getInitialState() {
    return {
      minimize: false
    };
  },

  handleResize(e) {
    this.state.minimize = window.innerWidth < 600;
    this.state.windowWidth = window.innerWidth;
  },

  componentDidMount() {
    window.addEventListener('resize', this.handleResize);
  },

  componentWillUnmount() {
    window.removeEventListener('resize', this.handleResize);
  },

  isAuthorized() {
    return !!this.props.isAuthorized;
  },

  isAuthenticated() {
    return !!this.props.isAuthenticated;
  },

  componentWillMount() {
    if (!this.isAuthenticated()) {
      this.props.dispatch(authHelper.authLogin(this.props.isAuthenticated));
    }
  },

  componentWillReceiveProps(nextProps) {
    let refreshToken = this.props.refreshToken;
    if (this.isAuthenticated() && this.isAuthorized() && !!refreshToken && !this.inactivityTimerSet) {
      this.props.dispatch(authHelper.setupInactivityTimer(refreshToken));
      this.inactivityTimerSet = true;
    }
  },

  render() {
    const token = authHelper.getLocalAuthToken();
    if ((this.isAuthenticated() && this.isAuthorized()) || token != null) {
      const version = packageJSON.version;
      const marginLeft = this.props.minimize ? '52px' : '177px';

      return !this.props.error ? (
        <div style={{height: '100%'}}>
          <div style={{height: '100%'}}>
            <LeftMenu version={version} minimize={this.props.minimize}/>
            <ErrorPage/>
            <div className="appMainBody" style={{
              width: document.body.clientWidth - 180,
              marginLeft: marginLeft,
              marginTop: '10px',
              paddingRight: '20px',
              paddingBottom: '20px'
            }}>
              {this.props.children}
            </div>
          </div>
          <Footer username={this.props.user.name}/>
        </div>
      ) : this.props.children;
    } else {
      return (
        <h4>Please wait, redirecting shortly ...</h4>
      );
    }
  }

});

export default connect(state => state.global)(App);
