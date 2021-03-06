/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        assertNull(lookup.get("foo"));
        Collection<String> names = lookup.getMatchingFieldNames("foo");
        assertNotNull(names);
        assertThat(names, hasSize(0));
    }

    public void testAddNewField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList(), Collections.emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
    }

    public void testAddFieldAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(field), Collections.singletonList(alias),
            Collections.emptyList());

        MappedFieldType aliasType = lookup.get("alias");
        assertEquals(field.fieldType(), aliasType);
    }

    public void testMatchingFieldNames() {
        MockFieldMapper field1 = new MockFieldMapper("foo");
        MockFieldMapper field2 = new MockFieldMapper("bar");

        FieldAliasMapper alias1 = new FieldAliasMapper("food", "food", "foo");
        FieldAliasMapper alias2 = new FieldAliasMapper("barometer", "barometer", "bar");

        FieldTypeLookup lookup = new FieldTypeLookup(List.of(field1, field2), List.of(alias1, alias2), List.of());

        Collection<String> names = lookup.getMatchingFieldNames("b*");

        assertFalse(names.contains("foo"));
        assertFalse(names.contains("food"));
        assertTrue(names.contains("bar"));
        assertTrue(names.contains("barometer"));

        Collection<MappedFieldType> fieldTypes = lookup.getMatchingFieldTypes("b*");
        assertThat(fieldTypes, hasSize(2));     // both "bar" and "barometer" get returned as field types
        Set<String> matchedNames = fieldTypes.stream().map(MappedFieldType::name).collect(Collectors.toSet());
        assertThat(matchedNames, contains("bar"));  // but they both resolve to "bar" so we only have one name
    }

    public void testSourcePathWithMultiFields() {
        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .addMultiField(new MockFieldMapper.Builder("field.subfield2"))
            .build(new ContentPath());

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(field), emptyList(), emptyList());

        assertEquals(Set.of("field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield1"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield2"));
    }

    public void testSourcePathsWithCopyTo() {
        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .build(new ContentPath());

        MockFieldMapper otherField = new MockFieldMapper.Builder("other_field")
            .copyTo("field")
            .build(new ContentPath());

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field, otherField), emptyList(), emptyList());

        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field.subfield1"));
    }

    public void testRuntimeFieldsLookup() {
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField runtime = new TestRuntimeField("runtime", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(concrete), emptyList(), List.of(runtime));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
    }

    public void testRuntimeFieldOverrides() {
        MockFieldMapper field = new MockFieldMapper("field");
        MockFieldMapper subfield = new MockFieldMapper("object.subfield");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField fieldOverride = new TestRuntimeField("field", "type");
        TestRuntimeField subfieldOverride = new TestRuntimeField("object.subfield", "type");
        TestRuntimeField runtime = new TestRuntimeField("runtime", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field, concrete, subfield), emptyList(),
            List.of(fieldOverride, runtime, subfieldOverride));
        assertThat(fieldTypeLookup.get("field"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("object.subfield"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
    }

    public void testRuntimeFieldsGetMatching() {
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper shadowed = new MockFieldMapper("field2");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2", "type");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield", "type");

        FieldTypeLookup fieldTypeLookup
            = new FieldTypeLookup(List.of(field1, shadowed, concrete), emptyList(), List.of(field2, subfield));
        {
            Set<String> matches = fieldTypeLookup.getMatchingFieldNames("fie*");
            assertEquals(2, matches.size());
            assertTrue(matches.contains("field1"));
            assertTrue(matches.contains("field2"));
        }
        {
            Collection<MappedFieldType> matches = fieldTypeLookup.getMatchingFieldTypes("fie*");
            assertThat(matches, hasSize(2));
            Map<String, MappedFieldType> toName = new HashMap<>();
            matches.forEach(m -> toName.put(m.name(), m));
            assertThat(toName.keySet(), hasSize(2));
            assertThat(toName.get("field2"), instanceOf(TestRuntimeField.class));
            assertThat(toName.get("field1"), instanceOf(MockFieldMapper.FakeFieldType.class));
        }
        {
            Set<String> matches = fieldTypeLookup.getMatchingFieldNames("object.sub*");
            assertEquals(1, matches.size());
            assertTrue(matches.contains("object.subfield"));
        }
    }

    public void testRuntimeFieldsSourcePaths() {
        //we test that runtime fields are treated like any other field by sourcePaths, although sourcePaths
        // should never be called for runtime fields as they are not in _source
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2", "type");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field1, concrete), emptyList(), List.of(field2, subfield));
        {
            Set<String> sourcePaths = fieldTypeLookup.sourcePaths("field1");
            assertEquals(1, sourcePaths.size());
            assertTrue(sourcePaths.contains("field1"));
        }
        {
            Set<String> sourcePaths = fieldTypeLookup.sourcePaths("field2");
            assertEquals(1, sourcePaths.size());
            assertTrue(sourcePaths.contains("field2"));
        }
        {
            Set<String> sourcePaths = fieldTypeLookup.sourcePaths("object.subfield");
            assertEquals(1, sourcePaths.size());
            assertTrue(sourcePaths.contains("object.subfield"));
        }
    }

    public void testFlattenedLookup() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(mapper), emptyList(), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(fieldName));

        String objectKey = "key1.key2";
        String searchFieldName = fieldName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertNotNull(searchFieldType);
        assertThat(searchFieldType, Matchers.instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class));
        FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());

        assertThat(lookup.getMatchingFieldNames("object1.*"), contains("object1.object2.field"));
        // We can directly find dynamic subfields
        assertThat(lookup.getMatchingFieldNames("object1.object2.field.foo"), contains("object1.object2.field.foo"));
        // But you can't generate dynamic subfields from a wildcard pattern
        assertThat(lookup.getMatchingFieldNames("object1.object2.field.foo*"), hasSize(0));
    }

    public void testFlattenedLookupWithAlias() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        String aliasName = "alias";
        FieldAliasMapper alias = new FieldAliasMapper(aliasName, aliasName, fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(mapper), singletonList(alias), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(aliasName));

        String objectKey = "key1.key2";
        String searchFieldName = aliasName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertNotNull(searchFieldType);
        assertThat(searchFieldType, Matchers.instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class));
        FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());
    }

    public void testFlattenedLookupWithMultipleFields() {
        String field1 = "object1.object2.field";
        String field2 = "object1.field";
        String field3 = "object2.field";

        FlattenedFieldMapper mapper1 = createFlattenedMapper(field1);
        FlattenedFieldMapper mapper2 = createFlattenedMapper(field2);
        FlattenedFieldMapper mapper3 = createFlattenedMapper(field3);

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(mapper1, mapper2), emptyList(), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));

        lookup = new FieldTypeLookup(Arrays.asList(mapper1, mapper2, mapper3), emptyList(), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));
        assertNotNull(lookup.get(field3 + ".some.key"));
    }

    public void testUnmappedLookupWithDots() {
        FieldTypeLookup lookup = new FieldTypeLookup(emptyList(), emptyList(), emptyList());
        assertNull(lookup.get("object.child"));
    }

    public void testMaxDynamicKeyDepth() {
        {
            FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            assertEquals(0, lookup.getMaxParentPathDots());
        }

        // Add a flattened object field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.emptyList(),
                Collections.emptyList()
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }

        // Add a short alias to that field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.singletonList(new FieldAliasMapper("alias", "alias", "object1.object2.field")),
                Collections.emptyList()
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }

        // Add a longer alias to that field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.singletonList(new FieldAliasMapper("alias", "object1.object2.object3.alias", "object1.object2.field")),
                Collections.emptyList()
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }
    }

    private FlattenedFieldMapper createFlattenedMapper(String fieldName) {
        return new FlattenedFieldMapper.Builder(fieldName).build(new ContentPath());
    }
}
