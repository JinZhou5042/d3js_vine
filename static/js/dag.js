import { fetchFile } from './tools.js';


export async function plotDAGComponentByID(dagID, generalStatisticsDAGCSV, logName) {
    try {
        const generalStatisticsDAG = d3.csvParse(generalStatisticsDAGCSV);
        const dag = generalStatisticsDAG.find(d => d.graph_id === dagID.toString());

        if (dag) {
            try {
                const svgElement = d3.select('#dag-components');
                svgElement.selectAll('*').remove();
            
                const svgContent = await d3.svg(`logs/${logName}/vine-logs/subgraph_${dagID}.svg`);
                svgElement.node().appendChild(svgContent.documentElement);
                const insertedSVG = svgElement.select('svg');
        
                insertedSVG
                    .attr('preserveAspectRatio', 'xMidYMid meet');

                // highlight the selected row
                const rows = document.querySelectorAll('#general-statistics-dag-table tbody tr');
                rows.forEach(row => {
                    if (+row.__data__.graph_id === dagID) {
                        row.style.backgroundColor = '#f2f2f2';
                    }
                });

            } catch (error) {
                console.error(error);
            }
        } else {
            console.error(`didn't find dag ${dagID}`);
        }
    } catch (error) {
        console.error('error when parsing ', error);
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    const selectDAG = document.getElementById('dag-id-selector');

    const { dagIDs, logName, generalStatisticsDAGCSV } = await fetchDAGIDs();

    dagIDs.forEach(dagID => {
        const option = document.createElement('option');
        option.value = dagID;
        option.text = `${dagID}`;
        selectDAG.appendChild(option);
    });

    selectDAG.addEventListener('change', async () => {
        const selectedDAGID = selectDAG.value;
        await plotDAGComponentByID(selectedDAGID, generalStatisticsDAGCSV, logName);
    });
});

async function fetchDAGIDs() {
    const logSelector = window.parent.document.getElementById('log-selector');
    const logName = logSelector.value;

    const generalStatisticsDAGCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_dag.csv`);
    const generalStatisticsDAG = d3.csvParse(generalStatisticsDAGCSV);
    const dagIDs = generalStatisticsDAG.map(dag => dag.graph_id);

    return { dagIDs, logName, generalStatisticsDAGCSV };
}
