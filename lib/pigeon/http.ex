defmodule Pigeon.HTTP do
  @moduledoc false

  alias Pigeon.HTTP.RequestQueue
  require Logger

  @type handler :: (Pigeon.HTTP.Request.t() -> any())
  @type reconnecter :: (term() -> {:noreply, term()} | {:stop, term()})

  @spec handle_info(term(), term(), handler()) :: {:noreply, term()}
  def handle_info(message, state, handler) do
    case handle_info(message, state, handler, &{:noreply, &1}) do
      {:noreply, state} -> {:noreply, state}
      {:stop, _reason} -> {:noreply, state}
    end
  end

  @spec handle_info(term(), term(), handler(), reconnecter()) ::
          {:noreply, term()} | {:stop, term()}
  def handle_info(message, state, handler, reconnect) do
    %{queue: queue, socket: socket} = state

    case Mint.HTTP.stream(socket, message) do
      :unknown ->
        {:noreply, state}

      {:ok, socket, responses} ->
        state
        |> process_responses(socket, responses, queue, handler)
        |> maybe_reconnect(reconnect)

      {:error, socket, error, responses} ->
        error |> inspect(pretty: true) |> Logger.error()

        state
        |> process_responses(socket, responses, queue, handler)
        |> maybe_reconnect(reconnect)
    end
  end

  defp process_responses(state, socket, responses, queue, handler) do
    {done, queue} =
      responses
      |> RequestQueue.process(queue)
      |> RequestQueue.pop_done()

    for {_ref, request} <- done do
      if request.notification, do: handler.(request)
    end

    %{state | queue: queue, socket: socket}
  end

  defp maybe_reconnect(%{socket: socket} = state, reconnect) do
    if Mint.HTTP.open?(socket) do
      {:noreply, state}
    else
      reconnect.(state)
    end
  end

  @spec timeout_pending_requests(term()) :: term()
  def timeout_pending_requests(%{queue: queue} = state) do
    {requests, queue} = RequestQueue.drain(queue)

    Enum.each(requests, fn request ->
      if request.notification do
        request.notification
        |> Map.put(:response, :disconnected)
        |> Pigeon.Tasks.process_on_response()
      end
    end)

    %{state | queue: queue}
  end
end
