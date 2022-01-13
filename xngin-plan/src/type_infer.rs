use xngin_catalog::DataType;

pub trait Typed {
    fn ty(&self) -> DataType;
}

pub trait TypeInfer<T> {
    fn infer(&self, target: &T) -> DataType;
}
