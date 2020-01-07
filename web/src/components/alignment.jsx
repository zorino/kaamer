import React from 'react';
import Box from '@material-ui/core/Box';

class Alignment extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            alnLines: [],
        };

        let bb = '';
        let queryString = "";
        let matchString = "";
        let sbjString = "";
        let queryStart = 0;
        let queryEnd = 0;
        let sbjStart = 0;
        let sbjEnd = 0;

        Array.from(this.props.seq_0).map((aa, i) => {
            bb = this.props.seq_2[i];
            if ((i)%60 == 0) {
                if (queryString != "") {
                    this.state.alnLines.push({
                        queryStart: queryStart,
                        queryEnd: queryEnd,
                        queryString: queryString,
                        matchString: matchString,
                        sbjStart: sbjStart,
                        sbjEnd: sbjEnd,
                        sbjString: sbjString
                    });
                }

                queryString = aa;
                sbjString = bb;
                matchString = this.props.seq_1[i];

                queryStart = queryEnd + 1;
                sbjStart = sbjEnd + 1;
                queryEnd += 1;
                sbjEnd += 1;

            } else {

                if (aa != "-") {
                    queryEnd += 1;
                }
                if (bb != "-") {
                    sbjEnd += 1;
                }

                queryString += aa;
                sbjString += bb;
                matchString += this.props.seq_1[i];

            }
        });
        if (queryString != "") {
            this.state.alnLines.push({
                queryStart: queryStart,
                queryEnd: queryEnd,
                queryString: queryString,
                matchString: matchString,
                sbjStart: sbjStart,
                sbjEnd: sbjEnd,
                sbjString: sbjString
            });
        }

    }


    getSpacingAln(queryNumber) {
        var output = "";
        for(var _i=1; _i<(4-queryNumber.toString().length); _i++) {
            output += " ";
        }
        return output;
    }

    render() {
        const alnStyle = {
            "font-family": "monospace",
            "font-size": "12px",
            background: "none",
            "line-height": "1",
            "border-radius": "0px",
            padding: "0px",
        };

        let aln = (
            <Box>
              <div style={{
                  "font-family": "Source Pro Sans",
                  "font-size": "8px"
              }}>
                <pre classes={alnStyle}>
                  {this.state.alnLines.map( l => (
                      <span>
                        Query {l["queryStart"]} {this.getSpacingAln(l["queryStart"])} {l["queryString"]}  {l["queryEnd"]}<br />
                        {"           "}{l["matchString"]}<br />
                        Sbjct {l["sbjStart"]} {this.getSpacingAln(l["sbjStart"])} {l["sbjString"]}  {l["sbjEnd"]}<br />
                        <br />
                      </span>
                  ))}
                </pre>
              </div>
            </Box>
        );


        return (
            <div id="alignment">
              { aln }
            </div>
        );
    }

};

export default Alignment;
