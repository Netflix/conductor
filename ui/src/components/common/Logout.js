import React, { Component } from 'react';
import { connect } from 'react-redux'
import * as authHelper from '../../core/AuthHelper';

class Logout extends Component {

  constructor(props) {
    super(props);
  }

  componentWillMount() {
    this.props.dispatch(authHelper.authLogout());
  }

  render() {
    return null;
  }
}

export default connect(state => state)(Logout);
