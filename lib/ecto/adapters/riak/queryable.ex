defmodule Ecto.RiakModel.Queryable do
  require Ecto.Model.Queryable, as: Super

  @doc false
  defmacro __using__(_) do
    quote do
      use Ecto.Query
      require Ecto.Model.Queryable
      import unquote(__MODULE__)
    end
  end
  
  defmacro queryable(source, entity) # do

  defmacro queryable(source, opts // [], do: block)

  defmacro queryable(source, opts, [do: block]) do
    quote do
      unquote(Super).queryable(unquote(source), unquote(opts), [do: unquote(add_riak_fields block)])
    end
  end
  
  defmacro queryable(source, [], entity) do
    quote do
      unquote(Super).queryable(unquote(source), [], unquote(entity))
    end
  end 

  ## ----------------------------------------------------------------------
  ## Private Helpers

  @type field_args :: [atom | Keyword.t]
  @type field      :: { :field, Keyword.t, field_args }
  @type block      :: { :__block__, Keyword.t, [field] }

  @spec add_riak_fields(field | block) :: [field]

  defp add_riak_fields({ :field, _, args } = field_block)
    when is_list(args) and length(args) >= 2 do
    add_riak_fields({ :__block__, [], [field_block] })
  end

  defp add_riak_fields({ :__block__, _, fields }) do    
    version_field = { :field, [], [:riak_version, :integer, default: 0] }
    vclock_field  = { :field, [], [:riak_vclock, :virtual] }
    context_field = { :field, [], [:riak_context, :virtual, default: []]  }
    fields = fields ++ [version_field, vclock_field, context_field]
    Enum.uniq(fields, fn { _, _, args } -> hd(args) end)
  end

end