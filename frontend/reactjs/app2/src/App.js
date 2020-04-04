import React from 'react';
import logo from './logo.png';
import pic from './covid-19.png';
import pic2 from './covid-19.jpeg';
import './App.css';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Información sobre el COVID-19 por el Complex System Lab (UPIITA)
        </p>
        <img src={pic} className="App-graph" alt="pic" />
        <br></br>
        <a
          className="App-link"
          href="https://sites.google.com/site/guzmanlev/covid19"
          target="_blank"
          rel="noopener noreferrer"
        >
          Google site
        </a>
      </header>
    </div>
  );
}

export default App;
