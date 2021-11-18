import React from 'react';
import Section from './Section.js';

export function FlexibleSearch() {
  return (
    <Section
      right={
        <>
          <h3>
            Flexible search
          </h3>
          <ul>
            <li>
              Precise search
            </li>
            <li>
              User filters
            </li>
          </ul>
        </>
      }
    />
  );
}