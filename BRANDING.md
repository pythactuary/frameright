# Schema Branding Assets

This directory contains the visual identity assets for Schema.

## Logo Files

### `logo.svg` - Full Logo (400x400)

The complete logo with animation, suitable for:

- Documentation homepages
- Project presentations
- Large displays

### `logo-icon.svg` - Icon Version (256x256)

Simplified square icon without text, perfect for:

- Favicon
- Package icons
- Social media avatars
- Small displays

### `logo-banner.svg` - Banner Version (1200x300)

Wide banner format for:

- README header
- Documentation headers
- Repository social preview

## Design Concept

The Schema logo represents:

1. **Shape-Shifting**: The rotating outer ring and morphing paths symbolize Proteus's ability to change form, reflecting the library's seamless transformation between Pandas and Polars backends.

2. **Structured Data**: The central grid represents DataFrame structure with highlighted cells showing type safety and validation.

3. **Modern & Professional**: Clean lines and a tech-forward color palette (cyan to turquoise gradient) convey reliability and innovation.

4. **Type Safety**: Dots in cells represent typed columns, emphasizing the library's IDE-first, type-safe approach.

## Color Palette

- **Primary Gradient**: `#009FDF` → `#00D9FF` → `#4DFFDF` (Cyan to Turquoise)
  - Represents data flow and transformation
- **Accent Gradient**: `#FF6B9D` → `#FFA07A` (Coral to Salmon)
  - Highlights and emphasis
- **Background**: `#0A1929` / `#132F4C` (Deep Navy)
  - Professional, technical feel

- **Text**: `#00D9FF` / `#4DFFDF` (Bright Cyan/Turquoise)
  - High contrast, readable

## Usage Guidelines

### In README

```markdown
![Schema](logo-banner.svg)
```

### As Favicon

Convert `logo-icon.svg` to ICO format or use directly in modern browsers:

```html
<link rel="icon" type="image/svg+xml" href="logo-icon.svg" />
```

### In Documentation

```html
<img src="logo.svg" alt="Schema Logo" width="200" />
```

## License

These branding assets are part of the Schema project and are released under the same MIT license.
