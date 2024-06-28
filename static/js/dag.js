import { fetchFile } from './tools.js';


export async function plotDAGComponentByID(dagID, generalStatisticsDAGCSV, logName) {
    const generalStatisticsDAG = d3.csvParse(generalStatisticsDAGCSV);
    const dag = generalStatisticsDAG.find(d => d.graph_id === dagID);
    if (dag) {
        try {
            const dagSVG = await fetchFile(`logs/${logName}/vine-logs/subgraph_${dagID}.svg`);
            if (dagSVG) {
                const svgContainer = document.getElementById('dag-components');
                svgContainer.innerHTML = dagSVG;

                const insertedSVG = svgContainer.querySelector('svg');
                
                const bbox = insertedSVG.getBBox();
                const viewBox = `${bbox.x} ${bbox.y} ${bbox.width} ${bbox.height}`;
                insertedSVG.setAttribute('viewBox', viewBox);
                insertedSVG.setAttribute('preserveAspectRatio', 'xMidYMid meet');
                
                const container = document.getElementById('dag-components-container');
                container.style.width = '100%';

                insertedSVG.setAttribute('width', '100%');
                insertedSVG.setAttribute('height', '100%');
            } else {
                console.error('Failed to fetch the SVG file');
            }
        } catch (error) {
            console.error('Error fetching the SVG file:', error);
        }
    } else {
        console.error(`DAG with ID ${dagID} not found`);
    }
}