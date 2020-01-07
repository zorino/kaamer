import React from 'react';
import axios from "axios";
import TextField from '@material-ui/core/TextField';
import FormControl from '@material-ui/core/FormControl';
import Button from '@material-ui/core/Button';
import Typography from '@material-ui/core/Typography';
import Container from '@material-ui/core/Container';
import Grid from '@material-ui/core/Grid';
import Box from '@material-ui/core/Box';
import SendIcon from '@material-ui/icons/Send';
import SubjectIcon from '@material-ui/icons/Subject';
import Paper from '@material-ui/core/Paper';

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

import { MuiThemeProvider, createMuiTheme } from '@material-ui/core/styles';
import Fab from '@material-ui/core/Fab';
import HighlightOffIcon from '@material-ui/icons/HighlightOff';
import CloseIcon from '@material-ui/icons/Close';

import Alignment from './alignment';

const theme = createMuiTheme({
    palette: {
        primary: {
            main: `#36454F`,
        },
    },
    overrides: {
        MuiPaper: {
            root: {
                padding: '0px',
            }
        },
        MuiTableCell: {
            root: {
                whiteSpace: 'nowrap',
                overflow: 'hidden',
            }
        },
        MuiExpansionPanel: {
            root: {
                'max-width': '1200px'
            }
        }
    },
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

    fetchKaamerResults() {

        this.setState({
            showResult: false,
            showProgress: true
        });

        let formData = new FormData();
        formData.append("type", "file");
        formData.append("gcode", "11");
        formData.append("output-format", "json");
        formData.append("align", "true");
        formData.append("annotations", "true");
        formData.append("positions", "false");
        formData.append("file", new Blob([this.state.fasta], { type: 'text/csv' }));

        axios
            .post(this.state.domain+"../api/search/protein", formData, {headers: {'Accept': '*/*'}})
            .then((res) => {
                this.setState({
                    showProgress: false,
                    showResult: true,
                    kaamerResults: res.data.results,
                    kaamerFeatures: res.data.dbProteinFeatures,
                });
                console.log(res);
            });

    }

    handleChange(event) {
        this.setState({fasta: event.target.value});
    }

    handleSubmit(event) {
        if (this.state.fasta != "") {
            this.fetchKaamerResults();
        }
        event.preventDefault();
    }

    keydownHandler(event){
        if(event.keyCode===13 && event.ctrlKey) {
            this.handleSubmit(event);
        }
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
                              <Typography style={{
                                  heading: {
                                      fontSize: theme.typography.pxToRem(15),
                                      fontWeight: theme.typography.fontWeightRegular,
                                  },
                              }}>
                                Query: {item.Query.Name}
                                <span style={{"margin-left": `20px`}}>
                                  |
                                </span>
                                <span style={{"margin-left": `20px`}}>
                                  Best Hit: {item.HitEntries[item.SearchResults.Hits[0].Key].EntryId} - {item.HitEntries[item.SearchResults.Hits[0].Key].Features["ProteinName"]} ({item.SearchResults.Hits[0].Alignment.Identity.toFixed(2)}%)
                                </span>
                              </Typography>
                            </ExpansionPanelSummary>
                            <ExpansionPanelDetails>
                              <Paper style={{
                                  'width': '100%',
                                  overflowX: 'auto'}} >
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
                                      {Object.entries(this.state.kaamerFeatures).map(([_, ft]) => (
                                          <TableCell>{ft}</TableCell>
                                      ))}
                                    </TableRow>
                                  </TableHead>
                                  <TableBody>
                                    {item.SearchResults.Hits.map(hit => (
                                        <TableRow>
                                          <TableCell style={{"max-width": "100%"}}>
                                            <Typography>
                                              <Button
                                                variant="contained"
                                                color="primary"
                                                endIcon={<SubjectIcon/>}
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

                                              <Box p={2}>
                                                <Typography variant="h5" component="h5">
                                                  Query: {item.Query.Name} | Hit: {item.HitEntries[hit.Key].EntryId}
                                                  <span style={{float: "right"}}>
                                                    <Fab aria-label="close" size="small" onClick={(e) => this.handlePopoverClose(e)}>
                                                      <CloseIcon />
                                                    </Fab>
                                                  </span>
                                                </Typography>
                                              </Box>

                                              <Box pl={2} pr={2} style={{"font-size": "6px !important"}}>
                                                <Table size="small">
                                                  <TableHead>
                                                    <TableRow>
                                                      <TableCell>Score</TableCell>
                                                      <TableCell>Expect</TableCell>
                                                      <TableCell>Identities</TableCell>
                                                      <TableCell>Positives</TableCell>
<TableCell>Gaps</TableCell>
                                                </TableRow>
                                              </TableHead>
                                              <TableBody>
                                                <TableCell>{hit.Alignment.BitScore.toFixed(2)}</TableCell>
                                                <TableCell><Typography noWrap>{hit.Alignment.EValue.toPrecision(2)}</Typography></TableCell>
                                                <TableCell>{hit.Alignment.Identity.toFixed(2)}%</TableCell>
                                                <TableCell>{hit.Alignment.Similarity.toFixed(2)}%</TableCell>
                                                <TableCell>{hit.Alignment.GapOpenings}</TableCell>
                                              </TableBody>
                                            </Table>
                                          </Box>

                                          <Alignment
                                                seq_0={hit.Alignment.AlnString.split("\n")[0]}
                                                seq_1={hit.Alignment.AlnString.split("\n")[1]}
                                                seq_2={hit.Alignment.AlnString.split("\n")[2]}
                                              />

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
                                          {Object.entries(this.state.kaamerFeatures).map(([_, ft]) => (
                                              <TableCell>{item.HitEntries[hit.Key].Features[ft]}</TableCell>
                                          ))}
                                        </TableRow>
                                    ))}
                                  </TableBody>
                                </Table>
                              </Paper>
                            </ExpansionPanelDetails>
                          </ExpansionPanel>
                      ))}
                    </Box>
            );
        }

        return (
            <MuiThemeProvider theme={theme}>

              <div style={{"margin-top":`20px`}} onKeyDown={(e) => this.keydownHandler(e)}>

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
