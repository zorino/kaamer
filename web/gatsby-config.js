require("dotenv").config({
    path: `.env`,
});

module.exports = {
    pathPrefix: process.env.SITE_PATH_PREFIX+'/web',
    siteMetadata: {
        title: 'kAAmer web search',
        description: 'Protein search in a kaamer database.',
        author: 'The kAAmer authors',
    },
    plugins: [
        'gatsby-theme-material-ui',
        'gatsby-plugin-react-helmet',
        {
            resolve: 'gatsby-source-filesystem',
            options: {
                name: 'images',
                path: `${__dirname}/src/images`,
            },
        },
        'gatsby-transformer-sharp',
        'gatsby-plugin-sharp',
        {
            resolve: 'gatsby-plugin-manifest',
            options: {
                name: 'gatsby-starter-default',
                short_name: 'starter',
                start_url: '/',
                background_color: '#663399',
                theme_color: '#663399',
                display: 'minimal-ui',
                icon: 'src/images/kaamer.svg', // This path is relative to the root of the site.
            },
        },
        'gatsby-plugin-google-analytics',
        {
            resolve: 'gatsby-plugin-google-analytics',
            options: {
                trackingId: process.env.GA_TRACKING_ID,
                head: false,
                anonymize: true,
                respectDNT: true,
            }
        },
    ],
};
