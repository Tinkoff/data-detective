import React from 'react';
import Section from './Section.js';

export function DataCatalogFeature() {
  return (
    <Section
      left={
        <>
          <h3>
            Data Catalog for entities of any type
          </h3>
          <ul>
            <li>
              Metadata loading of diagrams, tables, columns, reports, pipelines, DAGs, Zeppelin notes, Jupiter notebook is supported
            </li>
            <li>
              Relationship between entities of different types is supported
            </li>
          </ul>
        </>
      }
      right={
        <div id="gallery">
          <img src={require('@site/static/img/basic_card.gif').default} alt="basic-card" tabindex="0" />
          <div></div>
        </div>
      }
    />
  );
}