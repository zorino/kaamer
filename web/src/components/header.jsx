import PropTypes from "prop-types";
import React from "react";
import DescriptionIcon from '@material-ui/icons/Description';
import Grid from '@material-ui/core/Grid';
import Logo from '../images/kaamer_light.svg';

const Header = ({ siteTitle }) => (
    <header
      style={{
          background: `#36454F`,
          padding: `0.5em`,
      }}
    >
      <Grid container>
        <Grid item xs >
          <a href="../docs/#/"
             target="_blank"
             style={{
                 color: `white`,
                 textDecoration: `none`,
             }}
          >
            <DescriptionIcon style={{height: "100%"}} />
          </a>

        </Grid>
        <Grid item xs={6} style={{"text-align": "center"}}>
          <img src={Logo} alt={siteTitle} style={{width: "60px", "margin-bottom": "0px !important"}}/>
        </Grid>
        <Grid item xs>
        </Grid>

      </Grid>

    </header>
);

Header.propTypes = {
    siteTitle: PropTypes.string,
};

Header.defaultProps = {
    siteTitle: ``,
};

export default Header;
