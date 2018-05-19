/**
 *
 */
public class Driver {

    /**
     * This Driver set up and run all the jobs for the PageRank algorithm.
     *
     * It takes five arguments as input:
     * 1) the directory of a dataset file ---inPath
     * 2) the directory of transitionMatrix and PageRank files that you want to be saved ---fileOutput
     * 3) the directory of UnitMultiplication result  ---unitState
     * 4) the number of iterations of convergence ---count
     * 5) the damping factor (optional, default value: 0.85) ---damping
     *
     * The transition matrix will be saved under the dir of fileOutput/transition/
     * The PageRank file for each iteration will be saved under the dir of fileOutput/pagerank + i/
     * The final PageRank result in descending order will be saved under the dir of fileOutput/PageRankOrder/
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        PageRelationParser pageRelationParser = new PageRelationParser();
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();
        PageRankOrdering pageRankOrdering = new PageRankOrdering();

        String inPath = args[0];
        String fileOutput = args[1];
        String unitState = args[2];
        int count = Integer.parseInt(args[3]);
        String damping = args[4];

        String transitionMatrix = fileOutput + "/transition";
        String prMatrix = fileOutput + "/pagerank";
        String[] args0 = {inPath, fileOutput};
        pageRelationParser.main(args0);

        for(int i = 0; i < count; i++) {
            String[] args1 = {transitionMatrix, prMatrix + i, unitState + i, damping};
            multiplication.main(args1);
            String[] args2 = {unitState + i, prMatrix + i, prMatrix + (i + 1), damping};
            sum.main(args2);
        }

        String[] args3 = {prMatrix + count, fileOutput + "/PageRankOrder"};
        pageRankOrdering.main(args3);
    }
}
