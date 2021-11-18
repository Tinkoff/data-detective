import React from 'react';

export default function MainButton() {
  return (
    <div>
      <button type="button" className="main-button">
        <span className="button-wrapper" tabindex="-1">
          <span className="button-content">Welcome to documentation!</span></span>
      </button>
    </div>
  );
}