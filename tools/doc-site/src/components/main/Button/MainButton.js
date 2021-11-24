import React from 'react';
import Link from '@docusaurus/Link';

export default function MainButton() {
  return (
    <div>
      <button type="button" className="main-button">
        <span className="button-wrapper" tabindex="-1">
          <Link to="/docs/welcome">
            <span className="button-content">Welcome to documentation!</span>
          </Link>
        </span>
      </button>
    </div>
  );
}