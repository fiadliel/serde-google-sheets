use std::iter::Peekable;

use crate::error::{Error, Result};
use google_sheets4::api::{CellData, ExtendedValue, GridData};
use google_sheets4::hyper::client::HttpConnector;
use google_sheets4::hyper_rustls::HttpsConnector;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::Deserialize;

pub struct Deserializer<'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    rows: Peekable<I>,
    types: smallmap::Map<usize, Option<&'de str>>,
    key_idx: Option<usize>,
    cur_type: Option<&'de str>,
    parsing_enum: bool,
}

pub async fn from_spreadsheet<T>(
    sheets: &google_sheets4::Sheets<HttpsConnector<HttpConnector>>,
    spreadsheet_id: &str,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let spreadsheet = sheets
        .spreadsheets()
        .get(spreadsheet_id)
        .include_grid_data(true)
        .doit()
        .await?;

    let grid_data = spreadsheet
        .1
        .sheets
        .as_ref()
        .ok_or(Error::MissingSheet)?
        .get(0)
        .ok_or(Error::MissingSheet)?
        .data
        .as_ref()
        .ok_or(Error::NotGridSheet)?
        .get(0)
        .ok_or(Error::NotGridSheet)?;

    from_grid_data(grid_data)
}

pub fn from_grid_data<'a, T>(grid_data: &'a GridData) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut rows = grid_data
        .row_data
        .as_deref()
        .ok_or(Error::ZeroRows)?
        .iter()
        .map(|v| v.values.as_deref().expect("Values should be set"));

    let types: smallmap::Map<_, _> = rows
        .next()
        .ok_or(Error::ZeroRows)?
        .iter()
        .map(|v| v.formatted_value.as_deref())
        .enumerate()
        .collect();

    let mut deserializer = Deserializer {
        rows: rows.peekable(),
        types,
        key_idx: None,
        cur_type: None,
        parsing_enum: false,
    };

    T::deserialize(&mut deserializer)
}

impl<'de, I> Deserializer<'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    // Cannot be called when peek() returns None (i.e. after
    // end of all rows).
    fn get_cur_row_data(&mut self) -> &'de [CellData] {
        self.rows
            .peek()
            .expect("Need current row data with no selected row")
    }

    fn get_cur_cell_data(&mut self) -> Option<&'de CellData> {
        self.get_cur_row_data().get(self.key_idx.unwrap())
    }

    fn get_cur_effective_value(&mut self) -> Option<&'de ExtendedValue> {
        self.get_cur_cell_data()
            .and_then(|v| v.effective_value.as_ref())
    }

    fn deserialize_number(&mut self) -> Result<f64> {
        let value = self
            .get_cur_effective_value()
            .ok_or(Error::MissingValue)?
            .number_value
            .ok_or(Error::NotNumber)?;

        Ok(value)
    }

    fn deserialize_bool(&mut self) -> Result<bool> {
        let value = self
            .get_cur_effective_value()
            .ok_or(Error::MissingValue)?
            .bool_value
            .ok_or(Error::NotBool)?;

        Ok(value)
    }

    fn deserialize_formatted_value(&mut self) -> Result<&'de str> {
        self.get_cur_cell_data()
            .ok_or(Error::MissingValue)?
            .formatted_value
            .as_deref()
            .ok_or(Error::MissingValue)
    }
}

impl<'de, 'a, I> de::Deserializer<'de> for &'a mut Deserializer<'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.get_cur_effective_value() {
            Some(ExtendedValue {
                bool_value: Some(v),
                ..
            }) => visitor.visit_bool(v.to_owned()),
            Some(ExtendedValue {
                error_value: Some(_),
                ..
            }) => visitor.visit_borrowed_str(self.deserialize_formatted_value()?),
            Some(ExtendedValue {
                formula_value: Some(_),
                ..
            }) => visitor.visit_borrowed_str(self.deserialize_formatted_value()?),
            Some(ExtendedValue {
                number_value: Some(v),
                ..
            }) => {
                match self
                    .get_cur_cell_data()
                    .and_then(|v| v.effective_format.as_ref())
                    .and_then(|v| v.number_format.as_ref())
                    .and_then(|v| v.type_.as_ref())
                    .map(|v| v.as_str())
                {
                    Some("DATE" | "TIME" | "DATE_TIME") => {
                        visitor.visit_borrowed_str(self.deserialize_formatted_value()?)
                    }
                    _ => visitor.visit_f64(v.to_owned()),
                }
            }
            Some(ExtendedValue {
                string_value: Some(v),
                ..
            }) => visitor.visit_borrowed_str(v),
            Some(_) => todo!(),
            None => visitor.visit_none(),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.deserialize_bool()?)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.deserialize_number()? as i8)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.deserialize_number()? as i16)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.deserialize_number()? as i32)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.deserialize_number()? as i64)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.deserialize_number()? as u8)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.deserialize_number()? as u16)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.deserialize_number()? as u32)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.deserialize_number()? as u64)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(self.deserialize_number()? as f32)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(self.deserialize_number()?)
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self
            .get_cur_cell_data()
            .and_then(|v| v.formatted_value.as_deref())
            .unwrap();

        visitor.visit_borrowed_str(value)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.get_cur_effective_value() {
            None => visitor.visit_none(),
            Some(_) => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.get_cur_effective_value() {
            None => visitor.visit_unit(),
            Some(_) => Err(Error::ZeroRows),
        }
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = visitor.visit_seq(self)?;
        Ok(value)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(Enum::new(self))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.parsing_enum {
            let value = self
                .get_cur_cell_data()
                .and_then(|v| v.formatted_value.as_deref())
                .unwrap();

            visitor.visit_borrowed_str(value)
        } else {
            visitor.visit_borrowed_str(self.cur_type.unwrap())
        }
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

impl<'de, I> MapAccess<'de> for Deserializer<'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        let mut new_idx = match self.key_idx {
            None => 0,
            Some(i) => i + 1,
        };

        while self.types.get(&new_idx).map(|v| v.is_some()).is_none()
            && new_idx < self.types.len() - 1
        {
            new_idx += 1;
        }

        if new_idx >= self.get_cur_row_data().len() {
            return Ok(None);
        }

        self.key_idx = Some(new_idx);
        self.cur_type = Some(self.types.get(&new_idx).unwrap().unwrap());

        seed.deserialize(&mut *self).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }
}

impl<'de, I> SeqAccess<'de> for Deserializer<'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.rows.peek().is_none() {
            return Ok(None);
        }

        self.key_idx = None;
        self.cur_type = None;

        let val = seed.deserialize(&mut *self).map(Some);

        self.rows.next();

        val
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.types.len())
    }
}

struct Enum<'a, 'de: 'a, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    de: &'a mut Deserializer<'de, I>,
}

impl<'a, 'de, I> Enum<'a, 'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    fn new(de: &'a mut Deserializer<'de, I>) -> Self {
        Enum { de }
    }
}

impl<'a, 'de, I> EnumAccess<'de> for Enum<'a, 'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        self.de.parsing_enum = true;
        let val = seed.deserialize(&mut *self.de)?;
        self.de.parsing_enum = false;

        Ok((val, self))
    }
}

impl<'de, 'a, I> VariantAccess<'de> for Enum<'a, 'de, I>
where
    I: Iterator<Item = &'de [CellData]>,
{
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
fn string_cell(s: &str) -> CellData {
    CellData {
        formatted_value: Some(s.to_owned()),
        effective_value: Some(ExtendedValue {
            string_value: Some(s.to_owned()),
            ..ExtendedValue::default()
        }),
        ..CellData::default()
    }
}

#[cfg(test)]
fn grid_data(cells: Vec<Vec<CellData>>) -> GridData {
    GridData {
        row_data: Some(
            cells
                .iter()
                .map(|row| google_sheets4::api::RowData {
                    values: Some(row.clone()),
                })
                .collect(),
        ),
        ..GridData::default()
    }
}

#[test]
fn test_simple() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct Test {
        col1: String,
    }

    let data = grid_data(vec![
        vec![string_cell("col1")],
        vec![string_cell("Value in col 1")],
    ]);

    let expected = vec![Test {
        col1: "Value in col 1".to_owned(),
    }];

    let result: Vec<Test> = from_grid_data(&data).unwrap();

    assert_eq!(expected, result)
}
