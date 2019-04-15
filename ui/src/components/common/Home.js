import { Jumbotron } from 'react-bootstrap';
import React, { Component } from 'react';

class Introduction extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
        <Jumbotron className="jumbotron">
          <div className="row">
            <img src="/images/FRINX_logo_jumbotron.png"></img>
          </div>
          <div className="row">&nbsp;</div>
        </Jumbotron>
    );
  }
}

export default Introduction;
