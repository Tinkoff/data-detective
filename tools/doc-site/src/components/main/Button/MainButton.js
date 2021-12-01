import React from 'react';
import Link from '@docusaurus/Link';

export default function MainButton({text, link}) {
  return (
    <div>
      <button type="button" className="main-button">
        <span className="button-wrapper" tabindex="-1">
          <Link to={link}>
            <span className="button-content">{text}</span>
          </Link>
        </span>
      </button>
    </div>
  );
}
