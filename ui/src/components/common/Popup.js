import React, { Component } from 'react';
import unescapeJs from 'unescape-js';

class Popup extends Component {
    constructor(props) {
      super(props);  
      this.state = {
        text: unescapeJs(this.props.element.innerHTML)
      }
    }
  
    render() {
      return (
        <div className="popup">
          <div className="popup-inner">
            <div className="popup-content">
              <pre id="popup-editable-content" ref={elem => this.editor = elem} contentEditable>
                {this.state.text}
              </pre>          
            </div>
            <div className="popup-actions">
              <button className="btn btn-primary" onClick={() => this.props.closeAndSave(this.props.element, this.editor)} style={{float: 'right', marginBottom: '5px', marginRight: '10px'}}>Save</button>
              <button className="btn btn-default" onClick={this.props.close} style={{float: 'right', marginBottom: '5px', marginRight: '5px'}}>Cancel</button>  
            </div>
          </div>
        </div>
      );
    }
}

export default Popup