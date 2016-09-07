package PageRank;

public class PageRankDriver
{
	private static int times = 25;
	public static void main(String[] args)throws Exception
	{
		String[] forGB = {"./taskTransferOutput","./PRData0"};
		GraphBuilder.main(forGB);
		String[] forItr = {"./PRData0","./PRData1"};
		for(int i=0;i < times;i++)
		{
			forItr[0] = "./PRData" + i;
			forItr[1] = "./PRData" + String.valueOf(i + 1);
			PageRankIter.main(forItr);
		}
		String[] forRV = {"./PRData"  + times,"./FinalRank"};
		PageRankViewer.main(forRV);
	}
}