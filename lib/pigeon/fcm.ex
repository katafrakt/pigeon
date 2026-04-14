defmodule Pigeon.FCM do
  @moduledoc """
  `Pigeon.Adapter` for Firebase Cloud Messaging (FCM) push notifications.

  ## Getting Started

  ### Create a dispatcher.

    ```
    # lib/your_app/fcm.ex

    defmodule YourApp.FCM do
      use Pigeon.Dispatcher, otp_app: :your_app
    end
    ```

  ### Install and configure Goth.

  Install and configure [`goth`](https://hexdocs.pm/goth/1.4.3/readme.html#installation)
  if you haven't already. `Pigeon.FCM` requires it for token authentication.

  ### Configure your dispatcher.

  Configure your `FCM` dispatcher and start it on application boot.

  ```
  # config.exs

  config :your_app, YourApp.FCM,
    adapter: Pigeon.FCM,
    auth: YourApp.Goth, # Your Goth worker configured in the previous step.
    project_id: "example-project-123"
  ```

  Add it to your supervision tree.

  ```
  defmodule YourApp.Application do
    @moduledoc false

    use Application

    @doc false
    def start(_type, _args) do
      children = [
        {Goth, name: YourApp.Goth},
        YourApp.FCM
      ]
      opts = [strategy: :one_for_one, name: YourApp.Supervisor]
      Supervisor.start_link(children, opts)
    end
  end
  ```

  If preferred, you can include your configuration directly.

  ```
  defmodule YourApp.Application do
    @moduledoc false

    use Application

    @doc false
    def start(_type, _args) do
      children = [
        {Goth, name: YourApp.Goth},
        {YourApp.FCM, fcm_opts()}
      ]
      opts = [strategy: :one_for_one, name: YourApp.Supervisor]
      Supervisor.start_link(children, opts)
    end

    defp fcm_opts do
      [
        adapter: Pigeon.FCM,
        auth: YourApp.Goth,
        project_id: "example-project-123"
      ]
    end
  end
  ```

  ### Create a notification.

  ```
  n = Pigeon.FCM.Notification.new({:token, "reg ID"}, %{"body" => "test message"})
  ```

  ### Send the notification.

  On successful response, `:name` will be set to the name returned from the FCM
  API and `:response` will be `:success`. If there was an error, `:error` will
  contain a JSON map of the response and `:response` will be an atomized version
  of the error type.

  ```
  YourApp.FCM.push(n)
  ```

  ## Customizing Goth

  You can use any of the configuration options (e.g. `:source`) for Goth. Check out the
  documentation of [`Goth.start_link/1`](https://hexdocs.pm/goth/Goth.html#start_link/1)
  for more details.
  """

  @max_retries 3

  defstruct config: nil,
            ping_timer: nil,
            queue: Pigeon.HTTP.RequestQueue.new(),
            retries: @max_retries,
            socket: nil

  @behaviour Pigeon.Adapter

  import Pigeon.Tasks, only: [process_on_response: 1]

  alias Pigeon.Configurable
  alias Pigeon.FCM.{Config, Error}
  alias Pigeon.HTTP.{Request, RequestQueue}

  require Logger

  @impl Pigeon.Adapter
  def init(opts) do
    config = Pigeon.FCM.Config.new(opts)

    Configurable.validate!(config)

    state = %__MODULE__{config: config}

    case connect_socket(config) do
      {:ok, socket} ->
        state
        |> Map.put(:socket, socket)
        |> schedule_ping()
        |> then(&{:ok, &1})

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl Pigeon.Adapter
  def handle_push(notification, state) do
    with {:ok, %{config: config, queue: queue, socket: socket} = state} <-
           ensure_socket(state),
         headers = Configurable.push_headers(config, notification, []),
         payload = Configurable.push_payload(config, notification, []),
         method = "POST",
         path = "/v1/projects/#{config.project_id}/messages:send",
         {:ok, socket, ref} <-
           Mint.HTTP.request(socket, method, path, headers, payload) do
      new_q = RequestQueue.add(queue, ref, notification)

      state =
        state
        |> Map.put(:socket, socket)
        |> Map.put(:queue, new_q)

      {:noreply, state}
    else
      {:error, reason} ->
        {:stop, reason}

      {:error, socket, reason} ->
        {:stop, reason, %{state | socket: socket}}
    end
  end

  @impl Pigeon.Adapter
  def handle_info(:ping, state) do
    with {:ok, %{socket: socket} = state} <- ensure_socket(state),
         {:ok, socket, _ref} <- Mint.HTTP2.ping(socket) do
      state
      |> Map.put(:socket, socket)
      |> schedule_ping()
      |> then(&{:noreply, &1})
    else
      {:error, reason} ->
        {:stop, reason}

      {:error, socket, reason} ->
        {:stop, reason, %{state | socket: socket}}
    end
  end

  def handle_info(msg, state) do
    Pigeon.HTTP.handle_info(msg, state, &handle_response/1, &reconnect/1)
  end

  @spec handle_response(Request.t()) :: :ok
  def handle_response(%{body: body, notification: notif}) do
    body
    |> Pigeon.json_library().decode!()
    |> case do
      %{"name" => name} ->
        notif
        |> Map.put(:name, name)
        |> Map.put(:response, :success)
        |> process_on_response()

      %{"error" => error} ->
        notif
        |> Map.put(:error, error)
        |> Map.put(:response, Error.parse(error))
        |> process_on_response()
    end
  end

  @spec connect_socket(Config.t()) :: {:ok, Mint.HTTP2.t()} | {:error, term()}
  defp connect_socket(config), do: connect_socket(config, @max_retries)

  defp connect_socket(config, tries) do
    case Configurable.connect(config) do
      {:ok, socket} ->
        {:ok, socket}

      {:error, reason} ->
        if tries > 0 do
          connect_socket(config, tries - 1)
        else
          {:error, reason}
        end
    end
  end

  defp ensure_socket(%{socket: socket} = state) do
    if Mint.HTTP.open?(socket) do
      {:ok, state}
    else
      reconnect(state)
      |> reconnect_result()
    end
  end

  defp reconnect(%{config: config} = state) do
    state = Pigeon.HTTP.timeout_pending_requests(state)

    case connect_socket(config) do
      {:ok, socket} ->
        state
        |> Map.put(:socket, socket)
        |> schedule_ping()
        |> then(&{:noreply, &1})

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp reconnect_result({:noreply, state}), do: {:ok, state}
  defp reconnect_result({:stop, reason}), do: {:error, reason}

  defp schedule_ping(
         %{config: %{ping_period: ping_period}, ping_timer: timer} = state
       ) do
    if timer do
      Process.cancel_timer(timer)
    end

    %{state | ping_timer: Process.send_after(self(), :ping, ping_period)}
  end
end
