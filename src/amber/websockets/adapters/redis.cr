module Amber::WebSockets::Adapters
  # Allows websocket connections through redis pub/sub.
  class RedisAdapter
    @subscriber : Redis
    @publisher : Redis
    @listeners : Hash(String,Proc(String, JSON::Any, Nil)) = Hash(String, Proc(String, JSON::Any, Nil)).new
    @subscribed : Bool = false

    def self.instance
      @@instance ||= new
    end

    # Establish subscribe and publish connections to Redis
    def initialize
      @subscriber = Redis.new(url: Amber.settings.redis_url)
      @publisher = Redis.new(url: Amber.settings.redis_url)

      spawn do
        while true
          Fiber.yield
          if @subscribed == true
            to_subscribe = SUBSCRIBE_CHANNEL.receive
            @subscriber.subscribe(to_subscribe)
          end
        end
      end
    end

    # Publish the *message* to the redis publisher with topic *topic_path*
    def publish(topic_path, client_socket, message)
      @publisher.publish(topic_path, {sender: client_socket.id, msg: message}.to_json)
    end

    # Add a redis subscriber with topic *topic_path*
    def on_message(topic_path, listener)
      @listeners[topic_path] = listener
      spawn do
        begin
          @subscriber.subscribe(topic_path) do |on|
            on.message do |_, m|
              msg = JSON.parse(m)
              sender_id = msg["sender"].as_s
              message = msg["msg"]
              topic = message["topic"].to_s.split(":").first
              @listeners[topic].call(sender_id, message)
            end
            on.subscribe do |channel, subscription|
              Log.info { "Subscribed to channel #{channel}" }
              @subscribed = true
            end
            on.unsubscribe do |channel, subscription|
              Log.info { "Unsubscribed from channel #{channel}" }
              @subscribed = false
            end
          end
        rescue # only happens if crystal-redis is already subscribed to a channel
          SUBSCRIBE_CHANNEL.send(topic_path)
        end
      end
    end
  end
end