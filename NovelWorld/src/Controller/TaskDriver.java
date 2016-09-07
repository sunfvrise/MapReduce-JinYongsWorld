package Controller;

import PageRank.PageRankDriver;
import TagClassfier.TagClassfier;
import Txt2NameInfo.transferTask;

public class TaskDriver {

	public static void main(String[] args) throws Exception {
		transferTask.main(null);
		PageRankDriver.main(null);
		TagClassfier.main(null);
	}

}
