import React from 'react';
import Section from './Section.js';

export function AboutLineage() {
  return (
    <Section
      left={
        <>
          <h3>
            Two-level lineage
          </h3>
          <ul>
            <li>
              Lineage by tables and columns
            </li>
          </ul>
        </>
      }
    />
  );
}