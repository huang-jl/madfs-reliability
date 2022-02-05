use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(EpochRequest, attributes(epoch))]
pub fn derive_epoch_request(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    let mut epoch_fields = None;
    if let syn::Data::Struct(ref s) = input.data {
        if let syn::Fields::Named(ref names) = s.fields {
            // default
            if let Some(field) = names
                .named
                .iter()
                .find(|field| field.ident.as_ref().unwrap() == "epoch")
            {
                epoch_fields = Some(field.clone());
            }
            // the field which has epoch attributes
            let mut counter = 0;
            for field in names.named.iter() {
                if field.attrs.iter().any(|attr| {
                    attr.path
                        .get_ident()
                        .map_or(false, |ident| ident == "epoch")
                }) {
                    counter += 1;
                    if counter > 1 {
                        panic!("Only one field can has epoch attributes.");
                    }
                    epoch_fields = Some(field.clone());
                }
            }
        }
    } else {
        todo!("Do not implement for non-struct.")
    }
    let output = match epoch_fields {
        Some(field) => {
            let field_name = field.ident.unwrap();
            let struct_name = input.ident;
            quote! {
                impl EpochRequest for #struct_name {
                    fn epoch(&self) -> u64 {
                        self.#field_name
                    }
                }
            }
        }
        None => panic!("Please set `epoch` attributes to one field which means epoch."),
    };
    output.into()
}
