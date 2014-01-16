defmodule Ecto.RiakModel.Queryable do

  @riak_version_key      :riak_version
  @riak_version_type     :integer
  @riak_version_default  0

  @riak_vclock_key       :riak_vclock
  @riak_vclock_type      :binary
  @riak_vclock_default   nil

  @doc false
  defmacro __using__(_) do
    quote do
      use Ecto.Query
      import unquote(__MODULE__)
    end
  end

  defmacro queryable(source, entity)

  defmacro queryable(source, opts // [], do: block)

  defmacro queryable(source, opts, [do: block]) do
    ##IO.puts "queryable block #{inspect source}\n  #{inspect opts}\n  #{inspect add_riak_fields(block)}"
    quote do
      opts =
        (Module.get_attribute(__MODULE__, :queryable_defaults) || [])
        |> Keyword.merge(unquote(opts))
        |> Keyword.put(:model, __MODULE__)

      defmodule Entity do
        use Ecto.Entity, opts
        unquote(block)
      end

      queryable(unquote(source), Entity)
    end
  end

  defmacro queryable(source, [], entity) do
    quote do
      @ecto_source unquote(source)
      @ecto_entity unquote(entity)

      @doc "Delegates to #{@ecto_entity}.new/0"
      def new(), do: @ecto_entity.new()

      @doc "Delegates to #{@ecto_entity}.new/1"
      def new(params), do: @ecto_entity.new(params)

      @doc false
      def __model__(:source), do: @ecto_source
      def __model__(:entity), do: @ecto_entity

      @doc false
      def __queryable__,
        do: Ecto.Query.Query[from: { @ecto_source, @ecto_entity, __MODULE__ }]
    end
  end

  ## ----------------------------------------------------------------------
  ## Private Helpers

  @type field_args :: [atom | Keyword.t]
  @type field      :: { :field, Keyword.t, field_args }
  @type block      :: { :__block__, Keyword.t, [field] }

  @spec add_riak_fields(field | block) :: [field]

  defp add_riak_fields({ :field, _, args } = field_block)
    when is_list(args) and (length(args) == 2 or length(args) == 3) do
    add_riak_fields({ :__block__, [], [field_block] })
  end

  defp add_riak_fields({ :__block__, _, fields }) do    
    fields = maybe_add_field(fields, @riak_version_key, @riak_version_type, @riak_version_default)
    maybe_add_field(fields, @riak_vclock_key, @riak_vclock_type, @riak_vclock_default)
  end

  @spec maybe_add_field([field], atom, atom, term) :: [field]

  defp maybe_add_field(fields, name, type, default_value // nil) do
    ## Adds a field with a specified ${default_value}
    ## to a list of fields, but only if the field ${name}
    ## wasn't already present in ${fields}
    Enum.reduce(fields, [], fn { _f, _o, args } = field, acc ->
      [field_name, field_type | _] = args
      if name == field_name && type == field_type do
        [field | acc]
      else
        opts = field_options(args)
        opts = Keyword.put(opts, :default, default_value)
        new_args = [name, type, opts]
        [{ _f, _o, new_args } | acc]
      end
    end)
  end

  defp field_options([ _, _ ]), do: []
  defp field_options([ _, _, opts ]), do: opts

end