package test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class TestOptions {

	public static CommandLine getCommandLinde(String[] args) throws Exception {
		Options options = new Options();
		Option option = new Option("t", "table", true,
				"table to import into (must exist)");
		option.setRequired(true);
		option.setArgName("table-name");

		options.addOption(option);

		PosixParser posixParser = new PosixParser();
		CommandLine cmd = posixParser.parse(options, args);

		return cmd;
	}

	public static void main(String[] args) throws Exception{
		String[] args1 = new String[] { "-t", "table" };
		CommandLine cmd = getCommandLinde(args1);
		System.out.println(cmd.getOptionValue("t"));
		System.out.println(cmd.getOptions()[0].getArgName());
		
	}
}
