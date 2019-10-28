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
    this.setState({windowWidth: window.innerWidth, minimize: window.innerWidth < 600});
  },

  isAuthenticated() {
    return this.props.isAuthenticated === true;
  },

  isAuthorized() {
    return this.props.isAuthorized === true;
  },

  componentDidMount() {
    window.addEventListener('resize', this.handleResize);
  },

  componentWillUnmount() {
    window.removeEventListener('resize', this.handleResize);
  },

  componentWillMount() {
    if (!this.isAuthenticated()) {
      this.props.dispatch(authHelper.authLogin(this.state.isAuthenticated));
    }
  },

  componentWillReceiveProps(nextProps) {
    /*let refreshToken = nextProps.refreshToken;
    if (this.isAuthenticated() && this.isAuthorized() && !!refreshToken && !this.inactivityTimerSet) {
      this.props.dispatch(authHelper.setupInactivityTimer(refreshToken));
      this.inactivityTimerSet = true;
    }*/
  },

  getLoadingText() {
    if (this.state.isLoggedIn) {
      switch (this.state.authorizationStatus) {
        case 'successful':
          return 'You have been successfully authorized. Redirecting shortly ...';
        case 'forbidden':
          return 'Sorry, but you are not authorized to view this content.';
        default:
          return 'Please wait, while you are being authorized ...';
      }
    } else {
      return 'Please wait, redirecting shortly ...';
    }
  },

  render() {
    // if (!this.isAuthenticated() || !this.isAuthorized()) {
    //   return (
    //     <h4>{this.getLoadingText()}</h4>
    //   );
    // }

    const version = packageJSON.version;
    const marginLeft = this.state.minimize ? '52px' : '177px';
    return !this.props.error ? (
      <div style={{height: '100%'}}>
        <div style={{height: '100%'}}>
          <LeftMenu version={version} minimize={this.state.minimize}/>
          <ErrorPage/>
          <div className="appMainBody" style={{
            width: document.body.clientWidth - 180,
            marginLeft: marginLeft,
            marginTop: '10px',
            paddingRight: '20px'
          }}>
            {this.props.children}
          </div>
        </div>
        <Footer username={this.props.user.name}/>
      </div>
    ) : this.props.children;
  }

});

export default connect(state => state.global)(App);
