using System.CommandLine;
using Spectre.Console;

var rootCommand = new RootCommand("PollR console samples");
var commands = new Command[]
{
    new BasicTwoConsumerCommand(),
    new MultipleConsumerCommand(),
    new StoryModeCommand(),
};

foreach (var command in commands)
{
    rootCommand.Subcommands.Add(command);
}

rootCommand.SetAction(_ =>
{
    var table = new Table()
        .Title("[bold]Available PollR samples[/]")
        .AddColumn("Command")
        .AddColumn("Description");

    foreach (var command in commands)
    {
        table.AddRow($"[green]{command.Name}[/]", Markup.Escape(command.Description ?? ""));
    }

    AnsiConsole.Write(table);
    AnsiConsole.MarkupLine(
        "[grey]Run a sample with[/] [yellow]dotnet run -- <command> --runtime <seconds>[/]"
    );
});

return await rootCommand.Parse(args).InvokeAsync();
