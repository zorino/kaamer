import PropTypes from "prop-types";
import React from "react";
import DescriptionIcon from '@material-ui/icons/Description';

const Header = ({ siteTitle }) => (
    <header
      style={{
          background: `#36454F`,
          padding: `1em`
      }}
    >
      <div
        style={{
            margin: `0 auto`,
            maxWidth: 960,
            "text-align": `center`,
        }}
      >
        <h1 style={{ margin: 0 }}>
          <a href="../docs/#/"
             style={{
                 color: `white`,
                 textDecoration: `none`,
             }}
          >
            <DescriptionIcon />
          </a>


          <span
            style={{
                color: `white`,
                textDecoration: `none`,
                "margin-left": `20px`,
            }}
          >
            {siteTitle}
          </span>

        </h1>
      </div>
    </header>
);

Header.propTypes = {
    siteTitle: PropTypes.string,
};

Header.defaultProps = {
    siteTitle: ``,
};

export default Header;
