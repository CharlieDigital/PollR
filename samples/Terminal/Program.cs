using System.Net.Http.Json;
using System.Text;
using Terminal.Gui.App;
using Terminal.Gui.Drawing;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

var baseAddress = args.Length > 0 ? new Uri(args[0]) : new Uri("http://localhost:5055");
using HttpClient httpClient = new() { BaseAddress = baseAddress };
using CancellationTokenSource cancellationTokenSource = new();

var randomAdjectives = new[]
{
    "Quick",
    "Lazy",
    "Sleepy",
    "Noisy",
    "Hungry",
    "Happy",
    "Sad",
    "Angry",
    "Brave",
    "Clever",
};
var randomNouns = new[]
{
    "Fox",
    "Dog",
    "Cat",
    "Mouse",
    "Bear",
    "Orca",
    "Panda",
    "Koala",
    "Sloth",
    "Otter",
};

var randomVerb = new[]
{
    "Jumps",
    "Runs",
    "Sleeps",
    "Eats",
    "Barks",
    "Swims",
    "Climbs",
    "Dances",
    "Sings",
    "Reads",
};

var makeRandomMessage = () =>
{
    var adjective = randomAdjectives[Random.Shared.Next(randomAdjectives.Length)];
    var noun = randomNouns[Random.Shared.Next(randomNouns.Length)];
    var verb = randomVerb[Random.Shared.Next(randomVerb.Length)];
    return $"A {adjective} {noun} {verb}";
};

var channelSchemes = new Dictionary<string, Scheme>
{
    ["channel-1"] = CreateChannelScheme("BrightCyan", "Black"),
    ["channel-2"] = CreateChannelScheme("BrightGreen", "Black"),
    ["channel-3"] = CreateChannelScheme("BrightMagenta", "Black"),
};

ClientPanel[] clients =
[
    new("Client 1", "channel-1"),
    new("Client 2", "channel-1"),
    new("Client 3", "channel-2"),
    new("Client 4", "channel-2"),
    new("Client 5", "channel-3"),
];

using IApplication app = Application.Create();
app.Init();

using Window window = new()
{
    Title = $"PollR Terminal Demo - {baseAddress} (Esc to quit)",
    X = 0,
    Y = 0,
    Width = Dim.Fill(),
    Height = Dim.Fill(),
};

for (var index = 0; index < clients.Length; index++)
{
    var frame = CreateClientFrame(clients[index], index);
    frame.SetScheme(GetChannelScheme(clients[index].Channel));

    var output = new Label
    {
        X = 0,
        Y = 0,
        Width = Dim.Fill(),
        Height = Dim.Fill(),
        Text = $"Connecting to /events/{clients[index].Channel}...\n",
    };
    output.SetScheme(GetChannelScheme(clients[index].Channel));
    clients[index].Output = output;

    frame.Add(output);
    window.Add(frame);
}

var selectedChannel = clients[0].Channel;

var sendFrame = new FrameView
{
    Title = "Send Message",
    X = Pos.Percent(50),
    Y = Pos.Percent(66),
    Width = Dim.Fill(),
    Height = Dim.Fill(),
};

var channelLabel = new Label
{
    Text = "Channel:",
    X = 0,
    Y = 0,
};

var selectedLabel = new Label
{
    Text = selectedChannel,
    X = Pos.Right(channelLabel) + 1,
    Y = 0,
    Width = Dim.Fill(),
};
selectedLabel.SetScheme(GetChannelScheme(selectedChannel));

var messageLabel = new Label
{
    Text = "Message:",
    X = 0,
    Y = 2,
};

var messageInput = new TextField
{
    Text = makeRandomMessage(),
    X = Pos.Right(messageLabel) + 1,
    Y = 2,
    Width = Dim.Fill(10),
};

var status = new Label
{
    Text = "After a client is waiting, type a message and click a channel button to send.",
    X = 0,
    Y = 6,
    Width = Dim.Fill(),
};

var channelButtons = new[]
{
    CreateChannelButton("channel-1", 0),
    CreateChannelButton("channel-2", 14),
    CreateChannelButton("channel-3", 28),
};

var sendButton = new Button
{
    Text = "Send",
    X = Pos.Right(messageInput) + 1,
    Y = 2,
    IsDefault = true,
};

var clearButton = new Button
{
    Text = "Clear",
    X = 42,
    Y = 4,
};

sendButton.Accepted += async (_, _) =>
{
    await SendAsync(selectedChannel);
};

clearButton.Accepted += (_, _) =>
{
    foreach (var client in clients)
    {
        ClearPanel(client);
    }

    status.Text = "Panels cleared.";
    status.SetNeedsDraw();
    app.LayoutAndDraw(true);
};

sendFrame.Add(
    channelLabel,
    selectedLabel,
    messageLabel,
    messageInput,
    sendButton,
    clearButton,
    status
);

foreach (var button in channelButtons)
{
    sendFrame.Add(button);
}

window.Add(sendFrame);

var subscriptionTasks = clients
    .Select(client =>
        Task.Run(() => SubscribeAsync(client, app, httpClient, cancellationTokenSource.Token))
    )
    .ToArray();

try
{
    app.Run(window);
}
finally
{
    await cancellationTokenSource.CancelAsync();
    await Task.WhenAll(subscriptionTasks)
        .WaitAsync(TimeSpan.FromSeconds(2))
        .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
}

Button CreateChannelButton(string channel, int x)
{
    var button = new Button
    {
        Text = channel,
        X = x,
        Y = 4,
    };
    button.SetScheme(GetChannelScheme(channel));

    button.HasFocusChanged += (_, _) =>
    {
        if (button.HasFocus)
        {
            SelectChannel(channel);
        }
    };

    button.Accepted += async (_, _) =>
    {
        SelectChannel(channel);
        await SendAsync(channel);
    };

    return button;
}

void SelectChannel(string channel)
{
    selectedChannel = channel;
    selectedLabel.Text = channel;
    selectedLabel.SetScheme(GetChannelScheme(channel));
}

async Task SendAsync(string channel)
{
    var message = messageInput.Text?.ToString() ?? string.Empty;

    if (string.IsNullOrWhiteSpace(message))
    {
        status.Text = "Enter a message before sending.";
        return;
    }

    status.Text = $"Sending to {channel}...";

    try
    {
        using var response = await httpClient.PostAsJsonAsync(
            $"/messages/{channel}",
            new WriteMessage(message),
            cancellationTokenSource.Token
        );

        status.Text = response.IsSuccessStatusCode
            ? $"Sent to {channel}."
            : $"POST failed: {(int)response.StatusCode} {response.ReasonPhrase}";

        messageInput.Text = makeRandomMessage();
    }
    catch (OperationCanceledException)
    {
        status.Text = "Shutting down.";
    }
    catch (Exception exception)
    {
        status.Text = $"POST failed: {exception.Message}";
    }
}

static void ClearPanel(ClientPanel client)
{
    client.Messages.Clear();
    client.Messages.Add($"Waiting for events from /events/{client.Channel}.");

    if (client.Output is not null)
    {
        client.Output.Text = string.Join('\n', client.Messages);
        client.Output.SetNeedsDraw();
    }
}

static FrameView CreateClientFrame(ClientPanel client, int index) =>
    new()
    {
        Title = $"{client.Name} - {client.Channel}",
        X = index % 2 == 0 ? 0 : Pos.Percent(50),
        Y = index switch
        {
            0 or 1 => 0,
            2 or 3 => Pos.Percent(33),
            _ => Pos.Percent(66),
        },
        Width = Dim.Percent(50),
        Height = index < 4 ? Dim.Percent(33) : Dim.Fill(),
    };

Scheme GetChannelScheme(string channel) => channelSchemes[channel];

static Scheme CreateChannelScheme(string foreground, string background)
{
    var normal = new Terminal.Gui.Drawing.Attribute(foreground, background, "None");
    var focus = new Terminal.Gui.Drawing.Attribute(background, foreground, "None");

    return new(normal)
    {
        Focus = focus,
        HotNormal = normal,
        HotFocus = focus,
        Active = focus,
        HotActive = focus,
    };
}

static async Task SubscribeAsync(
    ClientPanel client,
    IApplication app,
    HttpClient httpClient,
    CancellationToken cancellationToken
)
{
    try
    {
        AppendLine(app, client, $"Waiting for events from /events/{client.Channel}.");

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/events/{client.Channel}");
        using var response = await httpClient.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken
        );

        response.EnsureSuccessStatusCode();
        AppendLine(app, client, "Connected.");

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var reader = new StreamReader(stream);

        var eventBuffer = new StringBuilder();
        while (!cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (line is null)
            {
                AppendLine(app, client, "Disconnected.");
                return;
            }

            if (line.Length == 0)
            {
                if (eventBuffer.Length > 0)
                {
                    AppendLine(app, client, eventBuffer.ToString().TrimEnd());
                    eventBuffer.Clear();
                }
                continue;
            }

            eventBuffer.AppendLine(line);
        }
    }
    catch (OperationCanceledException)
    {
        AppendLine(app, client, "Stopped.");
    }
    catch (Exception exception)
    {
        AppendLine(app, client, $"Subscription failed: {exception.Message}");
    }
}

static void AppendLine(IApplication app, ClientPanel client, string line)
{
    app.Invoke(() =>
    {
        client.Messages.Insert(0, line);
        if (client.Messages.Count > 50)
        {
            client.Messages.RemoveAt(client.Messages.Count - 1);
        }

        if (client.Output is not null)
        {
            client.Output.Text = string.Join('\n', client.Messages);
            client.Output.SetNeedsDraw();
        }
    });
}

sealed record WriteMessage(string Text);

sealed class ClientPanel(string name, string channel)
{
    public string Name { get; } = name;

    public string Channel { get; } = channel;

    public List<string> Messages { get; } = [];

    public Label? Output { get; set; }
}
