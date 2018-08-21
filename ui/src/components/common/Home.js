import { Jumbotron } from 'react-bootstrap';
import React from 'react';
import { connect } from 'react-redux';

import netflixImg from 'images/conductor.png';

const Introduction = () => (
  <Jumbotron className="jumbotron">
    <div className="row">
      <img src={netflixImg} />
    </div>
    <div className="row">&nbsp;</div>
  </Jumbotron>
);

export default connect(state => state)(Introduction);
