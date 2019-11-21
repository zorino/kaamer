import React, { useState } from 'react';
import axios from "axios";
import TextField from '@material-ui/core/TextField';
import FormControl from '@material-ui/core/FormControl';
import Button from '@material-ui/core/Button';
import Typography from '@material-ui/core/Typography';
import Container from '@material-ui/core/Container';
import Grid from '@material-ui/core/Grid';
import Box from '@material-ui/core/Box';
import SendIcon from '@material-ui/icons/Send';
import InfoIcon from '@material-ui/icons/Info';

import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';

import Popover from '@material-ui/core/Popover';
import CircularProgress from '@material-ui/core/CircularProgress';

import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';

import { MuiThemeProvider, createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
    palette: {
        primary: {
            main: `#36454F`,
        },
    },
    overrides: {
        MuiPaper: {
            root: {
                padding: '10px',
            }
        },
    }
});

class FastaForm extends React.Component {

    constructor(props) {
        super(props);

        this.state = {
            fasta: '',
            img: '',
            showResult: false,
            showProgress: false,
            kaamerResults: [],
            domain: "",
            openAnchor: false,
            anchorEl: null,
        };

        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handlePopoverOpen = this.handlePopoverOpen.bind(this);
        this.handlePopoverClose = this.handlePopoverClose.bind(this);

    }

    componetDidMount() {
        this.state.domain = window.location.host;
    }

    fetchKaamerResults(){

        this.setState({showProgress: true});

        let formData = new FormData();
        formData.append("type", "file");
        formData.append("gcode", "11");
        formData.append("output-format", "json");
        formData.append("align", "true");
        formData.append("annotation", "true");
        formData.append("positions", "true");
        formData.append("file", new Blob([this.state.fasta], { type: 'text/csv' }));

        axios
            .post(this.state.domain+"../api/search/protein", formData)
            .then((res) => {
                this.setState({kaamerResults: res.data});
                this.setState({showResult: true});
            });

    }

    handleChange(event) {
        this.setState({fasta: event.target.value});
    }

    handleSubmit(event) {
        this.fetchKaamerResults();
        event.preventDefault();
    }

    handlePopoverOpen(event, popoverId) {
        event.preventDefault();
        this.setState({
            openedPopoverId: popoverId,
            anchorEl: event.target,
        });
    }
    handlePopoverClose() {
        event.preventDefault();
        this.setState({
            openedPopoverId: null,
            anchorEl: null,
        });
    }

    // getBestHit(item) {

    // }

    render() {

        let kaamerRes = "";
        const { anchorEl, openedPopoverId } = this.state;

        if (this.state.showProgress) {
            (
                kaamerRes =
                    <Box>
                      <CircularProgress />
                    </Box>
            );
        };

        if (this.state.showResult) {
            (
                kaamerRes =
                    <Box>
                      {this.state.kaamerResults.map(item => (
                          <ExpansionPanel>
                            <ExpansionPanelSummary
                              expandIcon={<ExpandMoreIcon />}
                              aria-controls="panel1a-content"
                              id="panel1a-header">
                              <Typography>
                                Query : {item.Query.Name}
                              </Typography>
                              <Typography style={{"margin-left": `20px`}}>
                                |
                              </Typography>
                              <Typography style={{"margin-left": `20px`}}>
                                Best Hit: {item.HitEntries[item.SearchResults.Hits[0].Key].EntryId} ({item.SearchResults.Hits[0].Alignment.Identity.toFixed(2)}%)
                              </Typography>
                            </ExpansionPanelSummary>
                          <ExpansionPanelDetails>
                              <Table size="small">
                                <TableHead>
                                  <TableRow>
                                    <TableCell>Hit</TableCell>
                                    <TableCell>%Identity</TableCell>
                                    <TableCell>AlnLength</TableCell>
                                    <TableCell>Mismatches</TableCell>
                                    <TableCell>GapOpenings</TableCell>
                                    <TableCell>QueryStart</TableCell>
                                    <TableCell>QueryEnd</TableCell>
                                    <TableCell>HitStart</TableCell>
                                    <TableCell>HitEnd</TableCell>
                                    <TableCell>EValue</TableCell>
                                    <TableCell>BitScore</TableCell>
                                  </TableRow>
                                </TableHead>
                                <TableBody>
                                  {item.SearchResults.Hits.map(hit => (
                                      <TableRow>
                                        <TableCell>

                                          <Typography noWrap>
                                            <Button
                                              variant="contained"
                                              color="primary"
                                              endIcon={<InfoIcon/>}
                                              onClick={(e) => this.handlePopoverOpen(e, item.HitEntries[hit.Key].EntryId)}>
                                              {item.HitEntries[hit.Key].EntryId}
                                            </Button>
                                          </Typography>
                                          <Popover
                                            open={openedPopoverId === item.HitEntries[hit.Key].EntryId}
                                            onClose={this.handlePopoverClose}
                                            anchorEl={anchorEl}
                                            anchorOrigin={{
                                                vertical: 'bottom',
                                                horizontal: 'right',
                                            }}
                                            transformOrigin={{
                                                vertical: 'top',
                                                horizontal: 'center',
                                            }}
                                          >

                                            <Typography variant="h5" component="h5">
                                              Hit : {item.HitEntries[hit.Key].EntryId}
                                            </Typography>
                                            <List>
                                              {Object.entries(item.HitEntries[hit.Key].Features).map(([ft, val]) => (
                                                  <ListItem>
                                                    <Typography>
                                                      {ft} : {val}
                                                    </Typography>
                                                  </ListItem>
                                              ))}
                                            </List>
                                          </Popover>

                                        </TableCell>
                                        <TableCell>{hit.Alignment.Identity.toFixed(2)}</TableCell>
                                        <TableCell>{hit.Alignment.Length}</TableCell>
                                        <TableCell>{hit.Alignment.Mismatches}</TableCell>
                                        <TableCell>{hit.Alignment.GapOpenings}</TableCell>
                                        <TableCell>{hit.Alignment.QueryStart}</TableCell>
                                        <TableCell>{hit.Alignment.QueryEnd}</TableCell>
                                        <TableCell>{hit.Alignment.SubjectStart}</TableCell>
                                        <TableCell>{hit.Alignment.SubjectEnd}</TableCell>
                                        <TableCell><Typography noWrap>{hit.Alignment.EValue.toPrecision(2)}</Typography></TableCell>
                                        <TableCell>{hit.Alignment.BitScore.toFixed(2)}</TableCell>
                                      </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </ExpansionPanelDetails>
                          </ExpansionPanel>
                      ))}
                    </Box>
            );
        }

        return (
            <MuiThemeProvider theme={theme}>

              <div style={{"margin-top":`20px`}}>

                <Container fixed>

                  <form id="searchForm" onSubmit={this.handleSubmit}>

                    <Grid container xs={12} alignContent='center'>
                      <Grid container item xs={1}/>
                      <Grid container item xs={10}>
                        <FormControl fullWidth>
                          <TextField
                            id="standard-multiline-static"
                            label=">Fasta Input"
                            multiline
                            rows="4"
                            margin="normal"
                            width="75%"
                            value={this.state.fasta}
                            onChange={this.handleChange}
                            variant="outlined"
                            inputProps={{
                                style: {fontSize: 12, fontFamily: 'monospace',}
                            }}
                          />
                        </FormControl>
                      </Grid>

                      <Grid container item xs={1}>
                        <Button
                          variant="contained"
                          color="primary"
                          endIcon={<SendIcon/>}
                          type="submit"
                          style={{
                              height: "120px",
                              "margin-top": "16px",
                              "margin-bottom": "8px",
                          }}>
                        </Button>
                      </Grid>

                    </Grid>

                  </form>

                </Container>

                <Container>
                  <Grid container>
                    <Grid container item xs={12} justify="center">
                      { kaamerRes }
                    </Grid>
                  </Grid>
                </Container>
              </div>
            </MuiThemeProvider>
        );

    }
}

export default FastaForm;
