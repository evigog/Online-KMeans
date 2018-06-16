package evi_thesis.flink

import org.apache.commons.cli.{BasicParser,HelpFormatter, Options}

object ParseCLI {

  private final val fileOptionName = "file"
  private final val helpOptionName = "help"
  private val options = new Options()

  options.addOption( "help", false, "print this message")
  options.addOption( fileOptionName, true, "configuration file to use")

  def parseArgs(args: Array[String]): Option[String] ={
    val parser = new BasicParser()
    // parse the command line arguments
    val line = parser.parse(options, args)

    if (line.hasOption(helpOptionName)) {
      val formatter = new HelpFormatter()
      formatter.printHelp("options", options)
      sys.exit(1)
    }
    else if (line.hasOption(fileOptionName)){
      Some(line.getOptionValue(fileOptionName))
    } else {
      None
    }
  }
}
