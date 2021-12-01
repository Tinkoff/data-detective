import React from 'react';
import MainButton from './Button/MainButton';

export default function Quickstart() {
  return (
    <div className="row padding--lg main-section">
      <div className="col col--4 col--offset-4">
        <h3 className="text--center">
          Quickstart guide
        </h3>
        <p className="text--center">

        </p>
        <div className="text--center">
          <MainButton text="Go to quickstart documentation!" link="/docs/data-detective-airflow/quickstart-airflow"/>
        </div>
      </div>
    </div>
  );
}
