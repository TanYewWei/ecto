defmodule Ecto.Model.Validations do
  @moduledoc ~S"""
  Conveniences for defining module-level validations in models.

  This module provides two macros `validate` and `validatep` that
  wrap around `Ecto.Validator`. Let's see an example:

      defmodule User do
        use Ecto.Model

        queryable "users" do
          field :name, :string
          field :age, :string
          field :filename, :string
          field :format, :string
        end

        validate user,
               name: present(),
                age: present(message: "must be present"),
                age: greater_than(18),
               also: validate_attachments

        validatep validate_attachments(user),
           filename: has_format(~r/\w+/),
             format: member_of(~w(jpg gif png))
      end

  By calling `validate user`, a `validate(user)` function is defined
  that validates each attribute according to the given predicates.
  A special attribute called `:also` is supported, useful to wire
  different validations together.

  The validations can be executed by calling the `validate` function:

      User.validate(User.new)
      #=> [name: "can't be blank", age: "must be present"]

  This function returns a list with the validation errors, with the
  attribute as key and the error message as value. You can match on
  an empty list to know if there were validation errors or not:

      case User.validate(user) do
        []     -> # no errors
        errors -> # got errors
      end

  `validatep` works the same as `validate` but defines a private
  function. Note both macros can pass a function name as first
  argument which is the function to be defined. For `validatep`, we
  defined a `validate_attachments` function. All validation functions
  must receive the current entity as argument. We can call the
  `validate_attachments/1` locally as:

      validate_attachments(user)

  ## Predicates

  Validations are executed via a series of predicates:

      validate user,
        name: present(),
         age: present(message: "must be present"),
         age: greater_than(18),
        also: validate_attachments

  Each predicate above is going to receive the attribute being validated
  and its current value as argument. For example, the `present` predicate
  above is going to be called as:

      present(:name, user.name)
      present(:age, user.age, message: "must be present")

  Note that predicates can be chained together with `and`. The following
  is equivalent to the example above:

      validate user,
        name: present(),
         age: present(message: "must be present") and greater_than(18),
        also: validate_attachments

  The predicate given to `:also` is special as it simply receives the
  current record as argument. In this example, `validate_attachments`
  will be invoked as:

      validate_attachments(user)

  Which matches the API of the private `validate_attachments(user)`
  function we have defined below. Note all predicates must return a
  keyword list, with the attribute error as key and the validation
  message as value.

  ## Custom predicates

  By using `Ecto.Model.Validations`, all predicates defined at
  `Ecto.Validator.Predicates` are automatically imported into your
  model.

  However, defining custom predicates is easy. As we have seen in
  the previous section, a custom predicate is simply a function that
  receives a particular set of arguments. For example, imagine we want
  to change the predicates below:

      validatep validate_attachments(user),
         filename: has_format(~r/\w+/),
           format: member_of(~w(jpg gif png))

  To a custom predicate for image attachments:

      validatep validate_attachments(user),
         filename: image_attachment()

  It could be implemented as:

      def image_attachments(attr, value, opts \\ []) do
        if Path.extname(value) in ~w(jpg gif png) do
          []
        else
          [{ attr, opts[:message] || "is not an image attachment" }]
        end
      end

  Note that predicates can also be called over remote functions as
  long as it complies with the predicate API:

      validatep validate_attachments(user),
         filename: Image.valid_attachment

  ## Function scope

  Note that calling `validate` and `validatep` starts a new function,
  with its own scope. That said, the following is invalid:

      values = ~w(jpg gif png)

      validatep validate_attachments(user),
         filename: has_format(~r/\w+/),
           format: member_of(values)

  You can use module attributes instead:

      @values ~w(jpg gif png)

      validatep validate_attachments(user),
         filename: has_format(~r/\w+/),
           format: member_of(@values)

  On the plus side, it means you can also call other functions from
  the validator:

      validatep validate_attachments(user),
         filename: has_format(~r/\w+/),
           format: member_of(valid_formats)

      defp valid_formats(), do: ~w(jpg gif png)

 or even receive arguments:

     validatep validate_attachments(user, valid_formats \\ ~w(jpg gif png)),
        filename: has_format(~r/\w+/),
          format: member_of(valid_formats)

  or:

      validatep validate_attachments(user, validate_format),
         filename: has_format(~r/\w+/),
           format: member_of(~w(jpg gif png)) when validate_format

  """

  @doc false
  defmacro __using__(_) do
    quote do
      require Ecto.Validator
      import  Ecto.Validator.Predicates
      import  Ecto.Model.Validations
    end
  end

  @doc """
  Defines a public function that runs the given validations.
  """
  defmacro validate(function, keywords) do
    do_validate(:def, function, keywords, Module.get_attribute(__CALLER__.module, :ecto_entity))
  end

  @doc """
  Defines a private function that runs the given validations.
  """
  defmacro validatep(function, keywords) do
    do_validate(:defp, function, keywords, Module.get_attribute(__CALLER__.module, :ecto_entity))
  end

  defp do_validate(kind, { _, _, context } = var, keywords, entity) when is_atom(context) do
    do_validate(kind, { :validate, [], [var] }, keywords, entity)
  end

  defp do_validate(_kind, { _, _, [] }, _keywords, _entity) do
    raise ArgumentError, message: "validate and validatep expects a function with at least one argument"
  end

  defp do_validate(kind, { _, _, [h|_] } = signature, keywords, entity) do
    do_validate_var(h)

    quote do
      unquote(do_validate_opt(kind, signature, keywords, entity))

      Kernel.unquote(kind)(unquote(signature)) do
        Ecto.Validator.record unquote(h), unquote(keywords)
      end
    end
  end

  defp do_validate_opt(_kind, _signature, _keywords, nil) do
    nil
  end

  defp do_validate_opt(kind, { fun, meta, [h|t] }, keywords, entity) do
    signature = { fun, meta, [quote(do: unquote(h) = unquote(entity)[])|t] }

    quote do
      Kernel.unquote(kind)(unquote(signature)) do
        Ecto.Validator.record unquote(h), unquote(keywords)
      end
    end
  end

  defp do_validate_var({ _, _, context }) when is_atom(context), do: :ok
  defp do_validate_var(expr) do
    raise ArgumentError, message: "validate and validatep expects a function with a var " <>
                                  "as first argument, got: #{Macro.to_string(expr)}"
  end
end
